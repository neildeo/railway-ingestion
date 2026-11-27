# TRUST Ingestor

## Purpose

A long-running Rust application that ingests Network Rail TRUST events from a STOMP feed and persists them to Google BigQuery.

The ingestor runs on a Google Cloud Run Instance using Tokio for asynchronous I/O. It is a portfolio project intended to demonstrate real-time data engineering, protocol implementation, concurrent processing, fault tolerance and operational deployment.

The primary objective is to capture as much of the ephemeral TRUST feed as reasonably possible without allowing individual processing failures to compromise the continuous operation of the collector.

The application is deliberately **not responsible for interpreting railway movements or building analytical data models**. Those concerns belong to downstream processors.

## Core decisions

- Implement STOMP directly over TCP/TLS using Tokio, rather than adopting a STOMP client library.
- Use separate supervisors for the STOMP connection and BigQuery connection, keeping network receiving independent of persistence operations.
- Use STOMP 1.2 individual acknowledgements (`client-individual`).
- Store one BigQuery row per TRUST event, with one STOMP `MESSAGE` providing a logical batch of rows.
- Use the BigQuery Storage Write API default stream with at-least-once ingestion semantics.
- Preserve each original event as JSON, extracting only the minimal metadata needed for ingestion and auditing.
- Bound queue sizes and concurrent persistence operations.
- Use Google Cloud Storage as a dead-letter destination when BigQuery cannot accept a delivery.
- Acknowledge a STOMP delivery only after successful BigQuery or GCS persistence.
- If neither destination succeeds, log a high-severity failure, leave the delivery unacknowledged and continue operating.
- Handle duplicate records downstream rather than introducing ingestion-time deduplication.

## Application architecture

The following are conceptual module responsibilities. Exact Rust module and type definitions will evolve during implementation.

### `main` / `config`

Owns process startup and shutdown.

Responsibilities:

- Load configuration and retrieve Network Rail credentials from GCP Secret Manager.
- Initialise structured logging.
- Construct shared adapters and communication channels.
- Start and monitor top-level supervisors.
- Coordinate graceful process shutdown.

The application uses Application Default Credentials on Google Cloud. Its runtime service account already has Secret Manager access.

### `stomp`

Owns the STOMP protocol and TCP/TLS connection.

Responsibilities:

- Establish the TLS transport and perform the STOMP handshake.
- Negotiate protocol version and heartbeats.
- Subscribe to the relevant Network Rail topic.
- Parse frames from the TCP byte stream.
- Receive `MESSAGE` frames and publish complete deliveries to the processing pipeline.
- Send acknowledgements requested by the pipeline.
- Detect transport errors, heartbeat failures and disconnections.

The STOMP reader must remain responsive while persistence is underway.

The outbound writer has exclusive responsibility for writing protocol frames to the connection.

STOMP parsing must handle framing across arbitrary TCP reads, including frame delimiters, optional `content-length`, header escaping, multiple frames per read and heartbeat bytes.

### `stomp` supervisor

Owns the lifecycle of a STOMP session.

Responsibilities:

- Establish a session.
- Monitor its reader, writer and heartbeat responsibilities.
- Tear down unhealthy sessions.
- Reconnect with an appropriate retry strategy.
- Ensure acknowledgements cannot be sent on the wrong session.

A session is disposable. The process must survive normal connection failures.

A stable subscription identity may be required across reconnects, but broker-specific subscription durability and redelivery semantics must be verified rather than assumed.

### `trust` / `model`

Owns the translation from received broker deliveries to ingestion records.

Responsibilities:

- Parse each STOMP `MESSAGE` body as a JSON array of TRUST events.
- Retain the original JSON for each individual event.
- Extract selected fields from the TRUST envelope.
- Attach delivery-level ingestion metadata to each event.
- Produce a batch suitable for persistence.

The TRUST domain-specific body is not otherwise interpreted or transformed.

An empty event array produces no BigQuery rows but still requires appropriate delivery handling.

### `pipeline`

Owns message processing and persistence coordination.

Responsibilities:

- Receive complete deliveries from the STOMP side through a bounded channel.
- Prepare event-grain BigQuery batches.
- Dispatch persistence operations concurrently within configured limits.
- Handle unsuccessful BigQuery writes.
- Route deliveries to GCS when necessary.
- Notify the originating STOMP session after successful persistence.
- Record failures and relevant operational measurements.

The pipeline is process-owned, not session-owned. Outstanding persistence operations can finish after a STOMP connection fails.

A completion notification must never cause an acknowledgement on a different STOMP session.

### `storage::bigquery`

Owns interactions with the BigQuery Storage Write API.

Responsibilities:

- Maintain a reusable BigQuery client/session.
- Submit event batches to the default stream.
- Await and interpret append responses.
- Report success or failure to the pipeline.
- Recover from connection failures under independent supervision.

One nonempty STOMP delivery corresponds to one logical batch append. Actual gRPC submission and response coordination will be established during implementation.

### `storage::gcs`

Owns dead-letter persistence.

Responsibilities:

- Store the original STOMP delivery when BigQuery cannot persist it.
- Preserve sufficient metadata for future recovery.
- Use unique object names to prevent accidental overwriting.
- Report confirmed durability or failure.
- Limit concurrent operations and prevent unbounded resource consumption.

Recovery of dead-letter objects is outside the initial scope. A separate recovery tool can be developed later.

## Message flow and metadata

Each received STOMP `MESSAGE` contains a JSON array of zero or more TRUST events.

1. The STOMP reader receives and parses the complete frame.
2. The application records the frame reception time.
3. The frame's STOMP metadata and original body are retained.
4. The TRUST adapter parses its JSON array into individual events.
5. Each event becomes a prospective BigQuery row.
6. The pipeline submits the event batch to BigQuery.
7. Successful persistence causes an acknowledgement request to be sent to the originating STOMP session.
8. If BigQuery cannot persist the batch, the original delivery is written to GCS.
9. Successful GCS persistence also permits an acknowledgement.
10. If both destinations fail, the delivery is abandoned without ACK and the failure is logged prominently.

### Raw BigQuery grain

**One row per TRUST event**, not one row per STOMP frame.

A frame may contain events of several types. Events from the same frame share delivery metadata but retain their own source metadata.

### Timestamp conventions

Two timestamps are initially required as first-class ingestion metadata:

| Field               | Origin                    | Meaning                                                                        |
| ------------------- | ------------------------- | ------------------------------------------------------------------------------ |
| `event_queued_at`   | Network Rail TRUST header | When Network Rail reports queuing that event                                   |
| `event_received_at` | Ingestor clock            | When our client has received the complete STOMP delivery containing that event |

`event_queued_at` can differ between events in a batch. `event_received_at` is shared across events delivered together.

Domain-specific timestamps, such as actual, planned or public timetable times, remain inside the original JSON payload. The ingestor does not reinterpret them.

The raw record also retains the original event JSON, its position within the delivery, selected envelope metadata and relevant STOMP provenance.

Precise field types and nullability will be defined alongside the BigQuery schema.

Persistence completion and acknowledgement timestamps are operational measurements, not fields that need to be retroactively written into raw rows.

### Duplicate handling

An event may be persisted successfully but delivered again if its STOMP acknowledgement is lost.

This is acceptable.

Raw ingestion provides at-least-once semantics where persistence and broker delivery permit. Canonical event identity and deduplication are downstream responsibilities.

Neither STOMP message IDs nor ingestion timestamps should automatically be treated as unique railway event identifiers.

## Concurrency and failure policy

The application separates network receiving from persistence using Tokio tasks and channels.

### BigQuery processing

- A bounded `mpsc` channel carries received deliveries to persistence processing.
- A configured concurrency bound limits outstanding BigQuery operations.
- Temporary BigQuery congestion is absorbed by the queue.
- A failed BigQuery append triggers GCS fallback.
- A full or unavailable BigQuery queue triggers immediate GCS fallback.

The precise number of concurrent appends and buffered deliveries will be established through testing.

### GCS fallback

GCS persistence also has a configured concurrency bound.

If GCS cannot accept or persist the delivery within a short deadline, the application abandons the delivery without acknowledging it.

This failure must be prominent in logs and metrics.

The system must never \*silently\* discard messages, but **a deliberate priority is keeping the collector alive.** For an ephemeral feed with limited replay, exhausting memory or crashing the ingestor while attempting to retain every delivery may cause more total data loss than abandoning an isolated delivery.

### Session independence

The STOMP and BigQuery sessions have independent supervisors.

A BigQuery failure does not automatically restart STOMP. A STOMP failure does not automatically cancel outstanding BigQuery operations.

If persistence succeeds after the original STOMP session has ended, the write remains valid, but its acknowledgement cannot be transferred to the replacement session.

## STOMP heartbeats and reconnection

STOMP heartbeats are a protocol responsibility and are separate from application event traffic.

During connection establishment, the client and broker negotiate heartbeat intervals.

The implementation must:

- Advertise supported outgoing and incoming heartbeat intervals.
- Send heartbeat bytes during periods without outgoing frames when required by the negotiated interval.
- Treat incoming STOMP frames and heartbeat bytes as connection activity.
- Detect an unresponsive broker using an appropriate timeout and tolerance.
- Reconnect after connection or heartbeat failure.
- Avoid sending ACKs after the originating connection is no longer valid.

A heartbeat is not a normal STOMP frame; the parser and session logic must handle it accordingly.

### Broker-specific assumptions requiring verification

The following cannot be guaranteed from STOMP protocol semantics alone:

- Whether Network Rail supports durable subscriptions and what configuration they require.
- Whether unacknowledged deliveries are redelivered on the same connection.
- What happens to unacknowledged deliveries on reconnect.
- How long the broker retains pending or disconnected-consumer data.
- How broker message identifiers behave across redelivery.

The client must not rely on indefinite retention or guaranteed replay. These behaviours will be investigated during live integration testing.

## Testing strategy

Testing should exercise the real asynchronous application code while replacing external dependencies at well-defined boundaries.

### Unit tests

Cover deterministic behaviour:

- STOMP frame parsing and serialisation.
- Partial reads and multiple frames.
- Frame terminators, headers and heartbeats.
- TRUST event-array parsing and metadata extraction.
- Raw-row construction.
- Persistence routing decisions.

### Local asynchronous integration tests

Use Tokio's asynchronous test runtime and a local fake STOMP broker.

Verify:

- Connection handshake and subscription.
- Delivery of known frames.
- Heartbeat negotiation and timeout detection.
- Connection interruption and recovery.
- Acknowledgement only after confirmed persistence.
- No acknowledgement on failure.
- In-flight writes when a connection closes.

Use fake storage adapters to simulate successful, slow, failed and unavailable persistence.

Tests should also exercise bounded queues and concurrency limits, ensuring failure does not produce unbounded work.

### Live integration tests

Opt-in tests against Network Rail and Google Cloud.

These verify real connectivity, authentication, broker behaviour and persistence APIs.

They must not be part of ordinary PR unit-test execution.

## Implementation milestones

Each milestone should leave behind a working, independently testable component.

### 1. STOMP protocol implementation

Implement the STOMP frame parser and serialiser, TCP/TLS transport, handshake and subscription.

**Acceptance:** local fake-broker tests demonstrate a complete protocol exchange.

### 2. Live STOMP connectivity and lifecycle

Retrieve credentials, connect to Network Rail, receive live messages, implement heartbeat handling and reconnection, and emit controlled INFO-level summaries.

**Acceptance:** live messages can be received and inspected without storing domain data.

This milestone is diagnostic, not reliable ingestion. Its temporary ACK policy must be explicit.

### 3. Internal data model

Implement the transformation from complete STOMP deliveries to batches of event-grain records.

Extract selected envelope metadata, assign reception timestamps and preserve original JSON.

Design the corresponding BigQuery raw schema.

**Acceptance:** deterministic tests verify correct records for representative TRUST frames.

### 4. Concurrent processing and acknowledgement

Implement the bounded message channel, process-owned persistence coordination, session-scoped ACK handling and concurrency control.

Use a fake persistence adapter initially.

**Acceptance:** tests verify independent receiving, write completion, acknowledgement order, saturation and session failure.

### 5. BigQuery persistence

Implement and supervise the Storage Write API client using the default stream.

Submit batches, confirm append outcomes and enable acknowledgements after successful persistence.

**Acceptance:** live TRUST events appear in the raw BigQuery table at event grain.

### 6. GCS dead-letter fallback

Implement fallback on BigQuery failure or saturation, with bounded concurrency and high-severity reporting when both paths fail.

**Acceptance:** deliberate BigQuery failure leads to durable GCS storage and a valid STOMP acknowledgement; dual failure leaves the delivery unacknowledged without crashing the collector.

### 7. Operational deployment

Deploy the complete ingestor through the existing `railway-ingestion` CI/CD workflow to a Terraform-provisioned Cloud Run Instance.

Verify continuous operation, reconnects, heartbeat handling, resource consumption and operational logging.

**Acceptance:** the Instance receives and persists live TRUST traffic unattended, with observable recovery from expected failures.

## Out of scope

The initial implementation will not include:

- Interpretation of train journeys, movements, cancellations or lateness.
- Downstream analytics and canonical event modelling.
- Ingestion-time deduplication.
- Exactly-once persistence.
- Automatic replay or recovery from GCS dead-letter objects.
- A web interface or visualisation.
- Additional infrastructure services unless testing demonstrates a need.

## Development principles

1. Build the smallest useful increment and test it before proceeding.
2. Prefer explicit ownership and well-defined boundaries over unnecessary abstraction.
3. Keep transport, transformation and persistence concerns independent.
4. Avoid unbounded queues, tasks or retry loops.
5. Treat malformed input and unreliable external systems as expected operating conditions.
6. Preserve raw source data and accurate provenance.
7. Measure actual behaviour before optimising throughput or introducing infrastructure.

This README is an architectural blueprint. Concrete Rust types, dependency choices, numeric concurrency limits and individual implementation details will be refined as the milestones are developed.
