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

Weekly full snapshots provide a checkpoint and later allow reconstructed state to be validated against Network Rail's own full state. If the immutability assumption turns out to be wrong, or our replay logic is wrong, that reconciliation should expose the drift quickly.

Network Rail documentation:

- SCHEDULE feed: https://wiki.openraildata.com/index.php/SCHEDULE
- JSON format: https://wiki.openraildata.com/index.php/JSON_File_Format

## Deployment shape

There is one SCHEDULES Python deployable and one container image.

Terraform creates separate Cloud Run Jobs which use the same image with different runtime configuration. The two behavioural flags are:

```text
FULL_SNAPSHOT=true|false
REQUIRE_PUBLICATION=true|false
```

Update mode also supports an optional manual-recovery override:

```text
UPDATE_DAY=mon|tue|wed|thu|fri|sat|sun
```

`FULL_SNAPSHOT` selects the source artefact family:

- `true` -> full snapshot;
- `false` -> daily update.

When `FULL_SNAPSHOT=false`, `UPDATE_DAY` selects the weekday-specific Network Rail update slot. In normal scheduled operation it is unset, and the application derives yesterday's weekday automatically. For example, a Thursday run requests `toc-update-wed`. For manual recovery, `UPDATE_DAY=tue` explicitly requests `toc-update-tue`.

`UPDATE_DAY` is only valid for update mode. Supplying it with `FULL_SNAPSHOT=true` should be treated as invalid configuration rather than silently ignored.

`REQUIRE_PUBLICATION` expresses a source-system expectation only:

- `false` -> it is acceptable for Network Rail not to have published the expected artefact yet;
- `true` -> by this invocation, the publication is required to exist.

`REQUIRE_PUBLICATION` does **not** change GCS idempotency behaviour.

A likely schedule is:

```text
Daily update
  early run       FULL_SNAPSHOT=false  REQUIRE_PUBLICATION=false
  backup run      FULL_SNAPSHOT=false  REQUIRE_PUBLICATION=true

Weekly full
  early run       FULL_SNAPSHOT=true   REQUIRE_PUBLICATION=false
  backup run      FULL_SNAPSHOT=true   REQUIRE_PUBLICATION=true
```

The exact scheduler times are infrastructure configuration. The important behaviour is that the early run is tolerant of late source publication while the later run acts as the operational deadline.


## Update-day selection and recovery window

Network Rail update extracts are addressed by weekday rather than by an arbitrary calendar date. The update request uses a value such as:

```text
toc-update-mon
toc-update-tue
...
toc-update-sun
```

In ordinary scheduled operation the application derives the previous weekday automatically. This keeps the Cloud Run configuration simple and makes the normal daily job equivalent to the documented Network Rail usage.

The optional `UPDATE_DAY` override exists for manual recovery. If a scheduled ingest is missed, the module can be run locally or manually with an explicit weekday, for example:

```text
FULL_SNAPSHOT=false
UPDATE_DAY=tue
```

This asks Network Rail for the Tuesday update regardless of the current day.

The weekday interface creates a practical recovery window of roughly one week. The feed should not be treated as a historical archive: after the same weekday slot rolls around again, `toc-update-tue` may refer to the newer Tuesday rather than the missed one.

This is one reason the project takes weekly full snapshots. A recent full snapshot provides a bounded recovery checkpoint if an older delta can no longer be retrieved. In the normal case, the latest trusted full snapshot plus all subsequent sequential updates is sufficient to reconstruct present state.

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

No acquisition date is needed in the object path. The sequence number is the authoritative ordering coordinate and cross-relates full and update artefacts.

Quarantined conflicts live separately:

```text
schedules/
  quarantine/
    update/
      sequence=5246/
        2026-10-03T10-03-42.381927Z.json.gz
```

The quarantine timestamp records when our ingest observed the conflicting artefact. Microsecond precision makes accidental collisions negligible while remaining human-readable.

## Metadata provenance

There are three distinct metadata layers.

### 1. Network Rail feed metadata

Read from the first decompressed NDJSON record:

```text
Metadata.type
Metadata.sequence
timestamp
```

Other header fields such as owner and sender are useful provenance but are not central to ingest control flow.

### 2. HTTP / S3 object metadata

Read from the redirected S3 response headers:

```text
ETag
Last-Modified
Content-Length
```

These describe the published source object rather than the timetable semantics.

### 3. GCS custom object metadata

Selected values from both source layers are persisted onto the GCS object so most operational comparisons do not require reopening and decompressing the file.

Normal objects should retain at least:

```text
source_type
source_sequence
source_timestamp
source_etag
source_last_modified
source_content_length
```

The exact gzip object remains the authoritative raw evidence. GCS metadata is convenience and provenance information.

## Streaming model

The full snapshot is roughly 130 MiB compressed and more than 3 GiB decompressed, so the ingest must not materialise the whole artefact in memory.

The initial probe request is opened with streaming enabled. The probe follows the same restart-from-scratch principle for transient failures. If the short source stream fails before the first NDJSON record has been obtained, the failed response is discarded and a fresh probe request begins from byte zero. Non-transient failures are not retried.

The probe reads only enough compressed bytes to decompress the first NDJSON line. Compressed chunks are fed incrementally into a stateful gzip decompressor, while the resulting decompressed bytes are buffered only until the first newline is found. A defensive upper bound is applied to this decompressed header buffer so a malformed source cannot cause unbounded probing.

Once the first NDJSON record has been parsed:

1. capture the source HTTP metadata;
2. validate the timetable header;
3. derive `(type, sequence)`;
4. derive the final GCS object path;
5. check GCS for an object at that path;
6. close the probe response;
7. either no-op, quarantine, or begin a fresh transfer request.

Uploads and quarantines therefore use a second HTTP request rather than continuing the probe response.

Before any transfer begins, the fresh response's source metadata is compared with the metadata observed during the probe. If it differs, the operation fails because the core decision was made against a different source artefact.

If the metadata still matches, the compressed response body is copied chunk-by-chunk directly from the HTTP stream into the GCS writer. The transfer path does not decompress the timetable data.

The ingest therefore preserves the exact upstream gzip bytes while keeping memory use bounded.

## Core control flow

Every invocation follows the same GCS-side decision logic regardless of whether it is an early run, backup run, retry, manual invocation, or concurrent duplicate.

Conceptually:

```text
probe source artefact
  -> obtain source metadata
  -> partially decompress first NDJSON record
  -> close probe response
  -> validate header
  -> extract source type, sequence, timestamp
  -> derive GCS object path
  -> inspect GCS

GCS object absent
  -> make fresh source request
  -> verify source metadata still matches probe
  -> stream exact compressed bytes to create-only GCS object

GCS object present and provenance matches
  -> healthy no-op

GCS object present and provenance differs
  -> make fresh source request
  -> verify source metadata still matches probe
  -> stream exact compressed bytes to quarantine
  -> fail loudly
```

`REQUIRE_PUBLICATION` only affects the source-publication branch before normal ingest can proceed.

## Source publication behaviour

The early and backup runs differ only in how they react when the expected Network Rail publication is not yet available.

### `REQUIRE_PUBLICATION=false`

If the expected publication is absent or stale:

```text
emit structured WARNING
exit successfully
```

The later backup run is expected to self-heal the situation.

This warning is a candidate for a log-based alert because the event is important but the Cloud Run Job is deliberately healthy.

### `REQUIRE_PUBLICATION=true`

If the expected publication is still absent or stale:

```text
emit ERROR
exit non-zero
```

The existing blanket Cloud Run job-failure metric alert then fires. By this point the automatic recovery window has expired and manual intervention may be required later.

## Idempotency and concurrency

The ingest treats `(type, sequence)` as immutable source identity.

If an object already exists at the derived path and its persisted source provenance matches the currently served artefact, the invocation exits successfully without uploading anything.

This remains true even if `REQUIRE_PUBLICATION=false`. An unexpected duplicate invocation is not itself a data failure.

The actual GCS write must also be create-only using a generation precondition equivalent to:

```text
if_generation_match = 0
```

This protects against races where two Cloud Run executions both observe the object as absent.

If a writer loses that race:

```text
reload the winning object
compare persisted provenance

match
  -> concurrent duplicate; successful no-op

mismatch
  -> anomaly; fail loudly
```

## Checksums

The SCHEDULE ingest does not calculate an application-level CRC32C.

Unlike CORPUS, SCHEDULES supplies an explicit sequence identity. For normal operation we therefore use source metadata and `(type, sequence)` for idempotency rather than downloading a duplicate artefact purely to compare hashes.

GCS performs its own upload integrity checking and stores object checksums server-side.

If this source-immutability assumption proves false in practice, weekly full-snapshot reconciliation should reveal the drift and the ingest policy can be tightened later.

## Failure modes and responses

### HTTP / network / authentication failure

Existing shared HTTP functionality handles configured retries for transient failures.

If retries are exhausted:

```text
emit ERROR
exit non-zero
```

The existing Cloud Run failure metric alert fires.

### Publication not available yet

If the source has not yet published the expected artefact:

```text
REQUIRE_PUBLICATION=false
  -> structured WARNING
  -> log-based alert
  -> exit 0

REQUIRE_PUBLICATION=true
  -> ERROR
  -> exit non-zero
  -> existing Cloud Run failure alert
```

### Backup run finds a new artefact

This is normal mop-up behaviour.

```text
GCS object absent
-> upload normally
-> success
```

### Artefact already ingested

If `(type, sequence)` already exists and persisted provenance matches the current source object:

```text
healthy no-op
exit 0
```

This is the normal backup-run outcome when the early run succeeded.

### Same sequence, different source provenance

If the source serves the same `(type, sequence)` but `ETag`, `Last-Modified`, or other persisted provenance differs:

```text
stream incoming artefact to quarantine
attach conflict metadata
emit ERROR
exit non-zero
```

The trusted normal object is never overwritten automatically.

This requires manual investigation because it violates the project's source-immutability assumption.

### Malformed or unexpected timetable header

Examples:

- gzip cannot be partially decompressed;
- first line is not valid JSON;
- `JsonTimetableV1` is missing;
- `Metadata.type` is missing or does not match `FULL_SNAPSHOT`;
- `Metadata.sequence` is missing or invalid.

Response:

```text
do not land into normal raw namespace
emit ERROR
exit non-zero
```

### Source changes between probe and transfer

Uploads and quarantines make a fresh source request after the initial probe.

Before streaming begins, the fresh response metadata must match the metadata observed during the probe. If it differs, the decision made from the probe is stale and the transfer must not proceed.

```text
fresh source metadata differs from probe
  -> do not land or quarantine the new response
  -> fail loudly
  -> later invocation starts again from a fresh probe
```

This condition is distinct from a provenance conflict with an already-landed canonical object. Quarantine is used for the latter, not for a source artefact which changes during a single invocation.

### Streaming or GCS upload failure

Uploads and quarantines allow up to three complete transfer attempts for transient source-network or GCS failures.

Each transfer attempt starts from scratch:

```text
open fresh source request
  -> verify source metadata still matches the probe
  -> open fresh create-only GCS upload
  -> stream compressed bytes from byte zero
```

If a transient failure occurs while reading the source stream or writing to GCS, the current transfer attempt is abandoned. A retry opens both a new source response and a new GCS upload session and begins again from byte zero.

The ingest does not attempt application-level byte-range resume or splice a restarted source response into a partially completed GCS upload.

Only transient failures are retried. Errors which invalidate the assumptions of the operation fail immediately, including:

- authentication or permission failures;
- malformed source responses;
- missing required source metadata;
- probe-to-transfer source metadata mismatch;
- conflicting provenance after losing a concurrent create race.

If all three transfer attempts fail transiently:

```text
emit ERROR
exit non-zero
```

The Cloud Run Job itself is not relied upon for routine transient retries. A later scheduled or manual invocation remains safe because normal writes are create-only and the ingest is idempotent.

### Concurrent duplicate writers

Two executions may race after both observe the object as absent.

The GCS generation precondition allows only one create to succeed. The loser reloads the winner and compares provenance.

Matching provenance becomes a successful no-op; differing provenance becomes a loud failure.

### Missing sequence in downstream replay

Raw landing should preserve whatever artefacts Network Rail publishes, but downstream state reconstruction must not silently bridge sequence gaps.

For example:

```text
5246
5248
```

must not be treated as a valid replay chain without `5247`.

Sequence-continuity validation belongs to the BigQuery/Dataform state-reconstruction layer rather than the raw ingest itself.

## Monitoring model

Two alerting mechanisms are intentionally used for different semantics.

### Log-based alert

Used for the rare event:

```text
early run could not obtain the expected publication
```

The application emits a structured warning event and exits successfully because a later retry is expected to recover automatically.

### Cloud Run failure metric alert

Used for conditions which require intervention or indicate an unhealthy execution, including:

- exhausted HTTP retries;
- required publication still missing at the deadline run;
- malformed source header;
- quarantine conflict;
- failed upload.

This keeps warning-level source lateness distinct from actual job failure.

## Full snapshots and recovery

The daily update feed is the normal path for advancing timetable state.

Weekly full snapshots serve two purposes:

1. **checkpoint** — downstream replay can begin from a recent full state rather than an arbitrarily old baseline;
2. **validation** — reconstructed state can be compared with Network Rail's own full snapshot at the same sequence.

If reconstruction diverges from the full snapshot, investigate whether the cause is:

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
- validate schedule business semantics;
- decompress and persist the 3+ GiB NDJSON representation;
- calculate application-level checksums.

Those concerns belong to downstream BigQuery/Dataform processing.
