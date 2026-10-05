# SCHEDULES ingestion design

This document describes the raw-ingestion design for the Network Rail SCHEDULE feed used by the `railway-ingestion` project.

The scope here is deliberately narrow: fetch full and update SCHEDULE artefacts, validate their identity and provenance, and preserve the exact upstream gzip object in GCS. Parsing the timetable records into BigQuery and reconstructing current timetable state happen downstream.

## Source model

Network Rail publishes two relevant SCHEDULE artefact families:

- **full snapshots**: a complete snapshot of the current schedule database;
- **daily updates**: ordered deltas which advance one schedule state to the next.

Each file is gzip-compressed NDJSON. The first NDJSON record is a `JsonTimetableV1` header such as:

```json
{
  "JsonTimetableV1": {
    "classification": "public",
    "timestamp": 1790814631,
    "owner": "Network Rail",
    "Sender": {
      "organisation": "Rockshore",
      "application": "NTROD",
      "component": "SCHEDULE"
    },
    "Metadata": {
      "type": "update",
      "sequence": 5246
    }
  }
}
```

The important feed-level identity is:

```text
(type, sequence)
```

For this project we treat that pair as an immutable source identifier. This is an architectural assumption inferred from the sequential feed semantics rather than an explicit immutability guarantee in the source documentation.

The header `timestamp` is treated as a Unix timestamp describing the publication represented by the extract. For daily updates it is used to determine whether a weekday-addressed source slot has rolled forward to a sufficiently recent publication.

Weekly full snapshots provide a checkpoint and later allow reconstructed state to be validated against Network Rail's own full state. If the immutability assumption turns out to be wrong, or our replay logic is wrong, that reconciliation should expose the drift.

Network Rail documentation:

- SCHEDULE feed: https://wiki.openraildata.com/index.php/SCHEDULE
- JSON format: https://wiki.openraildata.com/index.php/JSON_File_Format

## Weekday-slot behaviour

Network Rail update extracts are addressed by weekday:

```text
toc-update-mon
toc-update-tue
...
toc-update-sun
```

These endpoints should be understood as **weekday slots**, not as requests for a specific historical date.

Observed behaviour shows that when a new publication for a weekday has not yet replaced the previous one, the endpoint may continue returning the object from the previous week rather than returning no object.

For example, on Sunday 4 October 2026:

```text
toc-update-sat
  -> sequence 5249
  -> timestamp 2026-10-04 00:30:33 UTC

toc-update-sun
  -> sequence 5243
  -> timestamp 2026-09-28 00:30:35 UTC
```

The Saturday slot had rolled forward to the current publication. The Sunday slot was still serving the previous week's publication.

The ingest therefore must distinguish:

```text
source unavailable or malformed
```

from:

```text
source healthy but weekday slot stale
```

A stale update publication is a normal source-lateness condition. Failure to obtain and parse any valid publication is not.

## Deployment shape

There is one SCHEDULES Python deployable and one container image.

Terraform creates separate Cloud Run Jobs which use the same image with different runtime configuration.

The primary behavioural flags are:

```text
FULL_SNAPSHOT=true|false
REQUIRE_FRESH_PUBLICATION=true|false
```

Update mode also supports an optional manual override:

```text
UPDATE_DAY=mon|tue|wed|thu|fri|sat|sun
```

`FULL_SNAPSHOT` selects the source artefact family:

- `true` -> full snapshot;
- `false` -> daily update.

When `FULL_SNAPSHOT=false`, `UPDATE_DAY` selects the weekday-specific Network Rail update slot.

In normal scheduled operation it is unset, and the application derives yesterday's weekday using the `Europe/London` calendar. For example, a Thursday run requests `toc-update-wed`.

For manual recovery or investigation:

```text
UPDATE_DAY=tue
```

explicitly requests `toc-update-tue`.

`UPDATE_DAY` is only valid for update mode. Supplying it with `FULL_SNAPSHOT=true` is invalid configuration.

### `REQUIRE_FRESH_PUBLICATION`

This flag applies the operational deadline policy to **daily update freshness**.

For an update request:

```text
false
  -> a stale weekday slot is tolerated;
     emit a structured warning and exit successfully

true
  -> a stale weekday slot is an operational failure;
     exit non-zero
```

The flag does not determine whether an HTTP object must exist. A failure to obtain a valid SCHEDULE publication at all is always an error.

Full snapshots do not currently use timestamp freshness as an ingest gate. Whatever valid full publication the endpoint currently serves is accepted and then subjected to the normal identity/provenance logic.

`REQUIRE_FRESH_PUBLICATION` does **not** change GCS idempotency behaviour.

A likely schedule is:

```text
Daily update
  early run       FULL_SNAPSHOT=false  REQUIRE_FRESH_PUBLICATION=false
  backup run      FULL_SNAPSHOT=false  REQUIRE_FRESH_PUBLICATION=true

Weekly full
  FULL_SNAPSHOT=true
```

The exact scheduler times are infrastructure configuration.

For updates, the important behaviour is that the early run tolerates a weekday slot which has not yet rolled forward, while the later run acts as the operational deadline.

## Update freshness

Freshness is a property of daily updates only.

The application:

1. determines the most recent calendar date matching the requested weekday in `Europe/London`;
2. takes midnight at the beginning of that date;
3. subtracts 24 hours to create a grace window;
4. compares the publication header timestamp against that lower bound.

Conceptually:

```text
freshness_floor =
  midnight at latest requested weekday
  minus 24 hours
```

The interval is lower-bounded only:

```text
header timestamp >= freshness_floor
  -> fresh

header timestamp < freshness_floor
  -> stale
```

The 24-hour grace window avoids overfitting the ingest to an exact Network Rail generation time while still clearly rejecting a weekday slot which is still serving the previous week's publication.

Full snapshots bypass this freshness check.

## Update-day selection and recovery window

In normal scheduled operation the application derives the previous weekday automatically.

For example:

```text
Thursday run
  -> request toc-update-wed
```

The optional `UPDATE_DAY` override exists for manual recovery and investigation:

```text
FULL_SNAPSHOT=false
UPDATE_DAY=tue
```

This asks Network Rail for the object currently occupying the Tuesday slot.

Because the upstream interface exposes weekday slots rather than arbitrary historical publications, this is not a general historical backfill mechanism.

Once a weekday slot has rolled over again, the earlier delta may no longer be obtainable from the live Network Rail endpoint.

This limitation is acceptable for this project because periodic full snapshots provide eventual recovery checkpoints. A missed delta can make a particular replay interval incomplete, but a later trusted full snapshot restores a complete current state.

## GCS layout

Normal raw objects are catalogued by feed type and sequence:

```text
schedules/
  full/
    sequence=5245/
      schedules.json.gz

  update/
    sequence=5246/
      schedules.json.gz
    sequence=5247/
      schedules.json.gz
```

No acquisition date is needed in the canonical object path.

The sequence number is the logical publication coordinate and cross-relates full and update artefacts.

Quarantined conflicts live separately:

```text
schedules/
  quarantine/
    update/
      sequence=5246/
        2026-10-03T10-03-42.381927Z.json.gz
```

The quarantine timestamp records when the ingest observed the conflicting artefact. Microsecond precision makes accidental collisions negligible while remaining human-readable.

Canonical objects are create-only. There is no normal overwrite path.

## Metadata provenance

There are three distinct metadata layers.

### 1. Network Rail feed metadata

Read from the first decompressed NDJSON record:

```text
Metadata.type
Metadata.sequence
timestamp
```

Their roles are distinct:

```text
(type, sequence)
  -> logical publication identity

timestamp
  -> update-publication freshness and provenance
```

Other header fields such as owner and sender are useful source information but are not central to ingest control flow.

### 2. HTTP / S3 object metadata

Read from the redirected source response headers:

```text
ETag
Last-Modified
Content-Length
```

These describe the physical source object rather than the timetable publication semantics.

They are treated as opaque provenance values. In particular, `ETag` is not assumed to be an MD5 checksum.

### 3. GCS custom object metadata

Selected values from both source layers are persisted on the GCS object:

```text
source_type
source_sequence
source_timestamp
source_etag
source_last_modified
source_content_length
```

The exact gzip object remains the authoritative raw evidence. GCS metadata exists to make operational comparisons cheap.

## Streaming model

The full snapshot is roughly 130 MiB compressed and more than 3 GiB decompressed, so the ingest must not materialise the whole artefact in memory.

### Probe request

The initial request is opened with streaming enabled.

The probe:

1. follows the authenticated Network Rail request to the source object;
2. captures the required HTTP metadata;
3. reads compressed chunks incrementally;
4. feeds them into a stateful gzip decompressor;
5. buffers decompressed output only until the first newline;
6. parses the first NDJSON record as `JsonTimetableV1`;
7. closes the response.

Compressed HTTP chunk boundaries have no semantic significance. A chunk may contain part of a DEFLATE structure, one record, or several records. The stateful decompressor retains the state necessary to continue across arbitrary chunk boundaries.

A defensive upper bound of 1 MiB is applied to the first decompressed NDJSON line. If no newline occurs within that limit, the publication is malformed.

A successful probe always returns a `SchedulePublicationInfo`.

It does **not** return `None` for late publication.

A source which cannot provide a valid, parseable SCHEDULE artefact is an error.

### Probe retries

If a transient source/network failure occurs before the first NDJSON record has been obtained, the failed probe is discarded and a fresh request begins from byte zero.

The decompressor and header buffer are recreated for each attempt.

Non-transient failures are not retried.

### Transfer request

If core decides that an upload or quarantine is required, the operation makes a new HTTP request rather than continuing the probe response.

Before transfer begins, all required source metadata from the fresh response must exactly match the metadata observed during the probe.

If it differs, the source changed between decision and transfer and the operation fails immediately.

If it matches, the compressed response body is copied chunk-by-chunk directly into the GCS writer.

The transfer path does not decompress the timetable.

The ingest therefore preserves the exact upstream gzip bytes while keeping application memory bounded.

## Core control flow

The core separates three concerns:

```text
source health
  -> can a valid publication be fetched and parsed?

publication freshness
  -> for updates, has the requested weekday slot rolled forward?

object provenance
  -> does this logical publication match any canonical object already stored?
```

The control flow is:

```text
probe source
  |
  v
valid source + valid JsonTimetableV1?
  |
  +-- no
  |     -> loud failure
  |
  +-- yes
        |
        v
validate header extract type
        |
        v
daily UPDATE only:
is publication timestamp fresh for requested weekday?
        |
        +-- stale + REQUIRE_FRESH_PUBLICATION=false
        |     -> structured warning
        |     -> no-op
        |     -> exit 0
        |
        +-- stale + REQUIRE_FRESH_PUBLICATION=true
        |     -> loud failure
        |
        +-- fresh
              |
              v
FULL snapshot:
skip freshness check
              |
              v
derive canonical path from (type, sequence)
              |
              v
does canonical GCS object exist?
        |
        +-- no
        |     -> fresh transfer request
        |     -> verify source metadata still matches probe
        |     -> create-only upload
        |     -> success
        |
        +-- yes
              |
              v
compare persisted source provenance
              |
              +-- same
              |     -> healthy no-op
              |
              +-- different
                    -> fresh transfer request
                    -> verify source metadata still matches probe
                    -> stream observed artefact to quarantine
                    -> loud failure
```

There is deliberately no canonical overwrite path.

The condensed rule is:

```text
new identity
  -> upload new

same identity + same provenance
  -> no-op

same identity + different provenance
  -> quarantine + fail

stale update
  -> warn/no-op before deadline
  -> fail after deadline

invalid or inconsistent source
  -> fail
```

## Full snapshot behaviour

Full snapshots are deliberately simpler than daily updates.

The endpoint's current valid full publication is accepted regardless of timestamp freshness.

The ingest then applies the normal identity/provenance rules:

```text
new (full, sequence)
  -> upload

existing identity + matching provenance
  -> no-op

existing identity + differing provenance
  -> quarantine + fail
```

A weekly invocation may therefore encounter the same full snapshot as the previous invocation and simply no-op.

If a new full publication arrives after that run, it will be picked up by a later invocation.

This is acceptable because the full feed is used as a backup and validation checkpoint rather than as the primary daily state-transition mechanism.

## Idempotency and concurrency

The ingest treats `(type, sequence)` as immutable logical source identity.

If a canonical object already exists and its persisted source provenance matches the publication currently served upstream, the invocation exits successfully without uploading anything.

The actual GCS write is create-only using a generation precondition equivalent to:

```text
if_generation_match = 0
```

This protects against races where two Cloud Run executions both observe the object as absent.

If a writer loses that race:

```text
reload winning object
compare persisted provenance

match
  -> concurrent duplicate
  -> successful no-op

mismatch
  -> anomaly
  -> loud failure
```

A precondition failure is not blindly retried as a transient error.

## Source mutation and quarantine

If the same `(type, sequence)` is observed with different physical source provenance:

```text
ETag
Last-Modified
Content-Length
```

the trusted canonical object is never overwritten.

Instead:

```text
stream newly observed artefact to quarantine
fail loudly
```

This preserves both versions for investigation.

This behaviour protects against unexpected in-place mutation even though such mutation is expected to be unusual for a sequential delta feed.

The source could in principle correct schedule state in a later delta rather than rewriting an earlier publication, but the ingest does not depend on that assumption for canonical immutability.

## Checksums

The SCHEDULE ingest does not calculate an application-level CRC32C.

For normal operation:

```text
(type, sequence)
```

provides logical identity, while:

```text
ETag
Last-Modified
Content-Length
```

provide source-object provenance.

GCS performs its own upload integrity checking and stores object checksums server-side.

The exact upstream gzip bytes are retained as raw evidence.

## Failure modes and responses

### Source/network failure

Transient failures are retried according to the ingest's bounded retry policy.

If retries are exhausted:

```text
emit ERROR
exit non-zero
```

Authentication, permission, malformed-response and other non-transient failures fail immediately.

### Stale daily update

The source is healthy and returns a valid update publication, but the requested weekday slot has not yet rolled forward into the accepted freshness window.

Early run:

```text
REQUIRE_FRESH_PUBLICATION=false
  -> structured WARNING
  -> exit 0
```

Deadline run:

```text
REQUIRE_FRESH_PUBLICATION=true
  -> ERROR
  -> exit non-zero
```

This is the normal representation of a late Network Rail update.

### No valid source publication

Examples include:

- source request fails permanently;
- response is not a valid gzip stream;
- stream ends before a valid first NDJSON record is obtained;
- first line is not valid JSON;
- required source metadata is absent or malformed.

These are not interpreted as ordinary publication lateness.

Response:

```text
emit ERROR
exit non-zero
```

### New artefact

If the canonical `(type, sequence)` object does not exist:

```text
make fresh source request
verify source metadata against probe
stream exact compressed bytes to create-only GCS object
exit 0
```

### Artefact already ingested

If the canonical object exists and persisted provenance matches:

```text
healthy no-op
exit 0
```

This is normal for duplicate invocations and for a full-snapshot run where the source has not yet advanced.

### Same sequence, different source provenance

If the same `(type, sequence)` is served with different physical provenance:

```text
stream newly observed artefact to quarantine
emit ERROR
exit non-zero
```

The canonical object is never overwritten automatically.

### Malformed or unexpected timetable header

Examples:

- `JsonTimetableV1` is missing;
- `Metadata.type` is missing or invalid;
- `Metadata.sequence` is missing or invalid;
- timestamp is missing or invalid;
- header extract type does not match the requested feed family.

Response:

```text
do not land into normal raw namespace
emit ERROR
exit non-zero
```

### Source changes between probe and transfer

Uploads and quarantines make a fresh source request after the initial probe.

Before streaming begins:

```text
fresh response provenance
```

must equal:

```text
probe response provenance
```

If it differs:

```text
do not land the changed response
do not quarantine it as the earlier conflict
fail loudly
```

A later invocation begins again from a new probe.

This condition is distinct from discovering that an already-landed canonical object has different provenance.

### Streaming or GCS upload failure

Uploads and quarantines allow up to three complete transfer attempts for transient source-network or GCS failures.

Each attempt begins from scratch:

```text
open fresh source request
  -> verify metadata still matches probe
  -> open fresh create-only GCS upload
  -> stream compressed bytes from byte zero
```

If a transient failure occurs midway through reading or writing:

```text
abandon attempt
start new HTTP request
start new GCS upload session
restart from byte zero
```

The ingest does not perform application-level byte-range resume.

Only transient failures are retried.

Errors which invalidate the assumptions of the operation fail immediately, including:

- authentication or permission failures;
- malformed source responses;
- missing required source metadata;
- probe-to-transfer metadata mismatch;
- conflicting provenance after losing a concurrent create race.

After three failed whole-transfer attempts:

```text
emit ERROR
exit non-zero
```

The Cloud Run Job itself is not relied upon for routine transient retries.

A later scheduled or manual invocation remains safe because canonical writes are create-only and the ingest is idempotent.

### Concurrent duplicate writers

Two executions may race after both observe the canonical object as absent.

The GCS generation precondition allows only one create to succeed.

The loser reloads the winner:

```text
matching provenance
  -> successful no-op

different provenance
  -> loud failure
```

## Sequence continuity

Sequence numbers provide the logical ordering of SCHEDULE publications and are retained as first-class metadata.

The raw ingest does not currently require the next update sequence to equal the previous stored sequence plus one.

That is deliberate.

The weekday endpoint is not directly addressable by arbitrary historical sequence, and periodic full snapshots provide eventual recovery checkpoints. A missed delta may therefore create a temporary gap without making permanent current-state recovery impossible.

Downstream replay must nevertheless not silently bridge such a gap.

For example:

```text
5246
5248
```

must not be interpreted as a complete delta chain containing `5247`.

Sequence-continuity validation belongs to the BigQuery/Dataform reconstruction layer, where full snapshots and updates can be evaluated together.

## Monitoring model

Two alerting mechanisms are used for different semantics.

### Log-based alert

Used when an early daily-update run finds that the weekday slot is still stale:

```text
event = schedule_publication_stale
```

Useful structured fields include:

```text
extract_type
update_day
sequence
timestamp
```

The application exits successfully because the later deadline invocation is expected to retry.

### Cloud Run failure metric alert

Used for conditions indicating an unhealthy or deadline-missed execution, including:

- stale update at the mandatory run;
- exhausted transient retries;
- authentication or permission failure;
- malformed source/header;
- source mutation conflict;
- probe-to-transfer mutation;
- failed upload or quarantine.

This keeps expected temporary source lateness distinct from actual job failure.

## Full snapshots and recovery

The daily update feed is the normal path for advancing timetable state.

Weekly full snapshots serve two purposes:

1. **checkpoint** — downstream replay can begin from a recent full state rather than an arbitrarily old baseline;
2. **validation** — reconstructed state can be compared with Network Rail's own full snapshot at the same sequence.

They also give the project eventual recovery from missing historical deltas which are no longer available through the weekday-slot interface.

If reconstructed state diverges from a trusted full snapshot, investigate whether the cause is:

- a missed or misordered delta;
- incorrect replay logic;
- unexpected source mutation;
- parsing/model defects.

Once a full snapshot is trusted, it can become the new downstream replay checkpoint.

## Deliberate non-goals of this ingest

The raw SCHEDULE ingest does not:

- parse individual schedules, associations or TIPLOC records beyond the first header row;
- apply `Create`, `Delete` or `Update` transactions;
- maintain current timetable state;
- maintain a mutable "latest sequence" pointer;
- provide arbitrary historical retrieval by sequence number;
- require update sequences to be contiguous at raw-ingest time;
- validate schedule business semantics;
- decompress and persist the 3+ GiB NDJSON representation;
- calculate application-level checksums.

Those concerns belong downstream or are deliberately avoided to keep the raw ingest simple, restartable and predominantly stateless.