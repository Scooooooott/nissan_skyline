This project utilized generative AI to some extent. For details on the contribution ratio of generative AI and human intervention in the project, please see `Gen-AI disclosure.md`


## I. Project Overview and Implementation Scope

This project implements a stateful real-time stream processor for attributing page-view events to advertising clicks. It consumes `page_views` and `ad_clicks` from Kafka and, for each page view, selects the most recent click from the same user within the preceding 30-minute event-time window.

### Technology Stack

- Java 21
- Spring Boot 3.2
- Spring Kafka
- SQLite

### Implemented Scope

The implementation includes:

- Concurrent Kafka consumers for the `page_views` and `ad_clicks` topics.
- A 30-minute event-time attribution window.
- Configurable allowed lateness from 0 to 15 minutes (default:2).
- Per-partition, monotonically advancing watermarks.
- Buffering of page views until their attribution results can be finalized.
- Emit-once output.
- Durable SQLite copies of click state, pending page views, watermarks, processed inputs, outputs, and dead-letter records.
- At-least-once Kafka consuming with idempotent SQLite writes.
- Recovery of active clicks, pending page views, and watermarks before Kafka consumers are started after an application restart.
- Deduplication of Kafka replays and duplicate click events.
- Detection and dead-lettering of conflicting `click_id` values.
- Periodic eviction and archival of expired click state and processed-input records.

### Implementation Assumptions

1. **Consistent topic partitioning**

   The `page_views` and `ad_clicks` topics must have the same number of partitions and must use the same partitioning algorithm based exclusively on `user_id`.


2. **Stable idempotency keys**

   `click_id` uniquely identifies a logical click, while `event_id` uniquely identifies a logical page view. Repeated identifiers are expected to contain the same business data. Conflicting data for an existing identifier is treated as invalid rather than as an update.


3. **Event-driven watermark progression**

   A partition’s effective watermark is:

   ```text
   maximum observed event time - allowed lateness
   ```

   Watermarks advance only when new events arrive. An idle partition does not advance automatically, so its pending page views remain buffered until a later event advances that partition’s watermark.


4. **Global watermark-based eviction**

   Click eviction uses the minimum observed watermark across known partitions so that state required by a slower partition is not removed prematurely. An inactive partition with an old watermark may consequently delay global state eviction.


## II. Evironment Setup and Start

### Prerequisites

- JDK 21
- Maven 3.9+
- Docker with Docker Compose
- Python 3.11+ with dependencies from `requirements.txt`

The Docker Compose environment provides:

- **Kafka** — input broker for the `page_views` and `ad_clicks` streams.
- **ZooKeeper** — coordination service required by the selected Kafka image.
- **Kafka UI** — optional topic and record inspection at [http://localhost:8080](http://localhost:8080).

SQLite is embedded and requires no separate service. Runtime data is stored under `./output`.

The commands below use the `dev` container, where Kafka is available at `kafka:29092`.

### Starting the Project

#### 1. Start the infrastructure and development container

```bash
docker compose up -d zookeeper kafka kafka-ui dev
docker compose ps
```

#### 2. Create the input topics

Both topics must use the same partition count.

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic page_views \
  --partitions 3 \
  --replication-factor 1

docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic ad_clicks \
  --partitions 3 \
  --replication-factor 1
```

Confirm their configuration:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic page_views

docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic ad_clicks
```

#### 3. Install the data-generator dependencies

```bash
docker compose exec dev pip install -r requirements.txt
```

#### 4. Build the Java project

```bash
docker compose exec dev mvn clean package
```

#### 5. Start the stream processor

Run this in a dedicated terminal:

```bash
docker compose exec dev mvn spring-boot:run
```

The processor restores durable state from SQLite before starting its Kafka listeners.

#### 6. Generate the sample events

Run this in another terminal after the processor has started:

```bash
docker compose exec dev python data_generator.py
```

The generator publishes the predefined normal, out-of-order, multiple-click, expired-click, late-event, and unattributed-page-view scenarios.


## III. Configuration Reference


| Category | Property | Configured value | Purpose |
|---|---|---:|---|
| Kafka | `kafka.bootstrap-servers` | `kafka:29092` | Kafka broker used by both input consumers. This address is valid inside the Docker Compose network. |
| Kafka | `kafka.topics.page-views` | `page_views` | Topic containing page-view events. |
| Kafka | `kafka.topics.ad-clicks` | `ad_clicks` | Topic containing advertising-click events. |
| Kafka | `kafka.consumer.group-id` | `stream-processor-group` | Consumer group used for offset tracking and partition assignment. |
| Kafka | `kafka.consumer.concurrency` | `3` | Maximum number of concurrent listener threads per listener container. Effective parallelism is limited by the topic partition count. |
| Watermark | `watermark.allowed-lateness-minutes` | `2` | Amount subtracted from the maximum observed event time to obtain the effective watermark. Values from 0 to 15 minutes are accepted. Events earlier than the effective watermark are dropped as late. |
| SQLite | `output.database.path` | `./output/attributed_page_views.db` | Location of the SQLite database containing output, processing state, watermarks, pending page views, idempotency records, and dead letters. |
| SQLite | `spring.datasource.url` | `jdbc:sqlite:${output.database.path}` | Builds the JDBC connection URL from the configured database path. |
| SQLite | `spring.datasource.driver-class-name` | `org.sqlite.JDBC` | JDBC driver used to access SQLite. |
| Connection pool | `spring.datasource.hikari.maximum-pool-size` | `1` | Restricts the application to one pooled SQLite connection, serializing database access and avoiding competing writers. |
| Connection pool | `spring.datasource.hikari.minimum-idle` | `1` | Keeps the single configured SQLite connection available in the pool. |
| Connection pool | `spring.datasource.hikari.connection-timeout` | `5000` ms | Maximum time to wait for a connection from the pool. This is separate from SQLite lock waiting. |
| Schema | `spring.sql.init.mode` | `always` | Runs database schema initialization whenever the application starts. The schema uses idempotent `CREATE TABLE/INDEX IF NOT EXISTS` statements. |
| Schema | `spring.sql.init.schema-locations` | `classpath:schema.sql` | SQL resource used to create the processor’s state and output tables. |
| SQLite durability | `output.database.journal-mode` | `WAL` | Enables SQLite write-ahead logging, providing durable commits while allowing readers to continue during writes. |
| SQLite durability | `output.database.synchronous` | `FULL` | Requests full SQLite synchronization for stronger durability guarantees before Kafka records are acknowledged. |
| SQLite locking | `output.database.busy-timeout-ms` | `5000` ms | Time SQLite waits for a locked database before returning a busy error. |
| Click history | `output.database.click-state-migration-interval-ms` | `3600000` ms | Runs click-state archival once per hour. Eligible non-active clicks are moved from `click_state` to `click_history`. |
| Input history | `processed-input-history.migration-interval-ms` | `86400000` ms | Runs processed-input archival once per day. The retention cutoff itself is fixed at 12 hours in the implementation. |
| Input history | `processed-input-history.migration-batch-size` | `500` | Maximum number of processed-input records migrated to history during one scheduled execution. |


## IV. Join Semantics

For each page view, the processor searches for clicks from the same user within the following inclusive event-time window:

```text
pageViewTime - 30 minutes <= clickTime <= pageViewTime
```

A click with the same event time as the page view is considered eligible.

When multiple clicks fall within the window, candidates are ordered by:

1. `event_time`, with the most recent event selected.
2. `click_id`, when multiple clicks have the same event time.

### Click Deduplication

`click_id` is the idempotency key for click events. Existing clicks are classified as follows:

| Classification | Condition | Behavior |
|---|---|---|
| `REPLAY` | Same `click_id`, Kafka partition, offset, `user_id`, `event_time`, and `campaign_id` | The click is treated as an already-processed Kafka record and is not added to state again. |
| `DUPLICATE` | Same `click_id` and business content, but received from a different Kafka source position | The duplicate is recorded as processed but is not added to click state again. |
| `CONFLICT` | Same `click_id` but different `user_id`, `event_time`, or `campaign_id` | The incoming record is rejected and persisted as a dead-letter event. |


## V. Watermark Logic

The processor maintains the maximum observed event time independently for each numeric Kafka partition:

```text
maxObservedEventTime[partition]
```

The value advances monotonically and never moves backwards when an out-of-order event is received.

The effective watermark is calculated as:

```text
effectiveWatermark[partition] = maxObservedEventTime[partition] - allowedLateness
```

The same numeric partition watermark is shared by the `page_views` and `ad_clicks` streams. This relies on both topics having the same partition count and the same `user_id`-based partitioning strategy.

### Allowed Lateness

Allowed lateness is configured through:

```yaml
watermark:
  allowed-lateness-minutes: 2
```

The supported range is 0–15 minutes. Values outside this range cause application initialization to fail.

An incoming event is considered too late when:

```text
eventTime < effectiveWatermark[partition]
```

An event whose timestamp is exactly equal to the effective watermark is still accepted.

Late clicks and page views are not included in attribution processing. They are recorded in the processed-input ledger with the `DROPPED_LATE` status.

### Page-view Finalization

Every accepted, non-late page view is first persisted with the `PENDING` status. It remains buffered while:

```text
pageViewTime > effectiveWatermark[partition]
```

A page view becomes eligible for final output when:

```text
pageViewTime <= effectiveWatermark[partition]
```

At that point, the processor:

1. Finds the latest eligible click within the 30-minute window.
2. Writes the attributed page view to the durable output table.
3. Changes the persistent pending state to `EMITTED`.
4. Removes the page view from the in-memory pending set.

If no eligible click exists, the page view is still emitted, with both attribution fields set to `null`.

An accepted click or page view that advances the maximum observed event time may also advance the watermark and finalize older pending page views in the same partition.

### Known Limitation: Idle Partitions

Watermarks are entirely event-driven. The current implementation does not include processing-time advancement or idle-partition detection.

If a partition stops receiving events:

- Its watermark does not advance.
- Page views beyond its current watermark may remain pending indefinitely.
- Its old watermark may delay click-state eviction because global eviction uses the minimum watermark across known partitions.

Supporting idle partitions would require an explicit idleness policy or an additional cross-partition watermark coordination mechanism.


## VI. State Model, Eviction, and Archival

### State Overview

The processor keeps a fast in-memory representation for event-time joins and a durable SQLite representation for restart recovery and idempotency.

| State | In-memory representation | Durable representation | Purpose |
|---|---|---|---|
| Click state | User-keyed sorted click sets | `click_state` | Latest-click lookup and active-state recovery |
| Pending page views | Partition-keyed sorted page-view sets | `pending_page_view` | Buffering until watermark finalization |
| Watermarks | Partition-keyed maximum event times | `watermark_state` | Late-data classification and page-view finalization |
| Processed inputs | No primary in-memory copy | `processed_input` | Kafka offset-level idempotency and processing audit |
| Final output | No required in-memory copy | `attributed_page_view` | Durable emit-once attribution output |
| Dead letters | No required in-memory copy | `dead_letter_event` | Invalid, conflicting, or retry-exhausted inputs |

SQLite is the durable source of state. The in-memory representation is rebuilt from active clicks, pending page views, and watermarks before Kafka listeners are started after an application restart.

Normal event processing keeps the two representations consistent through the staged-change transaction strategy described in the delivery semantics section.

### Click Eviction

Click-state eviction runs every 30 seconds.

The processor first obtains the minimum observed event time across all initialized partitions. The eviction cutoff is:

```text
evictionCutoff
    = minimumObservedEventTime - allowedLateness - 30-minute attribution window
```

Equivalently:

```text
evictionCutoff = minimumEffectiveWatermark - 30 minutes
```

Only clicks satisfying the following condition are evicted:

```text
click.eventTime < evictionCutoff
```

A click whose event time is exactly equal to the cutoff remains active.

Eviction is performed in two stages:

1. Eligible `ACTIVE` rows are transactionally marked as `EVICTED` in SQLite.
2. After the database transaction commits, the corresponding old clicks are removed from the in-memory state.

Using the minimum watermark prevents a faster partition from evicting click state that may still be required by a slower partition.

### Click-history Migration

Evicted click rows remain temporarily in `click_state` before being archived to `click_history`.

The migration uses an additional retention period derived from the difference between wall-clock time and the global minimum observed event time.

This provides a safety interval of 30–90 minutes before an evicted click is removed from the hot state table.

Only non-`ACTIVE` clicks older than the resulting cutoff are eligible. Migration is performed in bounded batches, with a source-code fallback batch size of 500 records.

For each batch, the processor:

1. Copies eligible rows into `click_history`.
2. Verifies that all selected click IDs exist in the history table.
3. Deletes the corresponding non-active rows from `click_state`.

The copy, verification, and deletion execute within one SQLite transaction.

### Processed-input History

The `processed_input` table is the hot idempotency ledger for recently consumed Kafka records.

Records become eligible for archival after 12 hours. The cutoff uses:

```text
processed_at, when available
received_at, otherwise
```

Eligible rows are moved to `processed_input_history` in bounded batches. Each migration:

1. Selects the oldest eligible records.
2. Copies them into `processed_input_history`.
3. Verifies that every selected composite key exists in history.
4. Deletes the corresponding source rows from `processed_input`.

The copy and deletion are executed in the same SQLite transaction, keeping the archival operation idempotent and preventing partially migrated batches.

Archiving limits the size of the actively queried idempotency table while retaining historical processing information for audit and investigation.


## VII. Output and Delivery Semantics

### Emit-once Output

The processor uses emit-once semantics. Final attribution results are stored in the SQLite `attributed_page_view` table.

Once a page view has been finalized:

- No re-attribution update is produced.
- No correction or retraction is emitted.
- A subsequently arriving click does not modify the existing output.
- The existing output is never silently overwritten.

A page view without an eligible click is still emitted, with both `attributed_campaign_id` and `attributed_click_id` set to `null`.

### Pending State Versus Final Output

Receiving a valid page view does not necessarily produce an output immediately.

If the effective watermark has not yet reached the page view’s event time, the page view is durably stored as `PENDING`. The Kafka input can then be acknowledged because the information required for later finalization has been persisted, even though no final attribution output exists yet.

Only after the database transaction commits successfully is the event processing considered successful.

### Kafka Acknowledgment Order

For successfully processed input, the ordering is:

```text
SQLite transaction commit
    → apply committed changes to in-memory state
    → acknowledge the Kafka record
```

If the SQLite transaction fails, its changes are rolled back and the Kafka record is not acknowledged. Spring Kafka can then retry the record according to the configured error-handling policy.

Kafka offset commits and SQLite transactions are not part of one distributed transaction. The processor therefore provides:

```text
at-least-once input processing with idempotent durable output
```

### Commit and Acknowledgment Failure Boundary

SQLite and Kafka do not participate in one distributed transaction. A failure may therefore occur after the SQLite transaction has committed but before Kafka successfully commits the acknowledged offset.

In this case, Kafka may redeliver the same input record. The replay does not create another final output because the processor uses:

- The Kafka topic, partition, and offset as the processed-input identity.
- `click_id` as the click-state idempotency key.
- `page_view_id` as the pending-state and final-output idempotency key.
- SQLite unique constraints and content comparison for duplicate output writes.

An identical output replay is treated as successful. A replay that attempts to produce different content for an existing `page_view_id` is rejected as an output conflict rather than overwriting the stored result.

### Output Idempotency

`page_view_id` is the primary idempotency key of the output table.

When an output with the same `page_view_id` already exists:

- If every stored output field matches the newly generated result, the write is accepted as an idempotent replay.
- If the content or attribution differs, the write fails with an output conflict.
- The existing output is never replaced by the conflicting result.

### Transactional Memory Consistency

The processor maintains both durable SQLite state and in-memory state used for low-latency attribution. Updating the shared in-memory state before the database transaction commits would create a consistency risk: if the transaction were rolled back, later events could observe clicks, watermarks, or pending-page-view changes that were never durably persisted.

To prevent this, each input record is processed with a private in-memory change set. During the SQLite transaction, click additions, watermark advancement, pending-page-view additions, and finalized-page-view removals are staged in this change set rather than applied directly to the shared state.

Attribution decisions use the existing locked state together with the staged input, allowing the transaction to calculate the correct result without exposing uncommitted changes to other workers.

The processing order is:

```text
stage database and in-memory changes
    → commit the SQLite transaction
    → apply the staged changes to shared memory
    → acknowledge the Kafka record
```


### Offset Ownership

Kafka consumer-group offsets remain the authoritative source of the input position for each topic partition.

The SQLite `processed_input` table is an application-level idempotency and audit ledger. It records the terminal processing result of an input record, but it does not replace Kafka’s consumer-group offset storage.

A processed input is identified by:

```text
(topic, partition, offset)
```

This allows the processor to recognize Kafka replays independently of the business identifiers used by clicks and page views.


### Manual Acknowledgment

Kafka automatic offset commits are disabled:

```text
enable.auto.commit = false
```

Both listener containers use Spring Kafka’s `MANUAL_IMMEDIATE` acknowledgment mode.

For normal processing, `Acknowledgment.acknowledge()` is invoked only after the join engine has returned successfully. At that point, the required SQLite transaction has committed and the committed changes have been applied to the in-memory state.

If an unexpected exception is thrown:

- The record is not acknowledged by the listener.
- Spring Kafka retains or seeks back to the failed offset.
- The configured error handler controls retry and recovery.

Terminal invalid-input and conflict paths are acknowledged only after their error information has been durably persisted.


### Dead Letters

Late data is an expected event-time condition rather than a processing failure. It is therefore not written to `dead_letter_event` and does not enter the retry path.

Dead-letter storage is reserved for cases such as:

- Malformed or incomplete Kafka records.
- Conflicting business data for an existing `click_id`.
- Processing failures that still fail after the configured retries.


### Retry and Backoff

Unexpected processing failures are handled by Spring Kafka’s `DefaultErrorHandler`.

The implementation currently uses the following fallback defaults:

```text
kafka.consumer.max-retries = 3
kafka.consumer.retry-backoff-ms = 1000
```

These values are supported by the application but are not explicitly declared in the current `application.yml`.

A failed record is retried up to three times after the initial processing attempt, with a one-second fixed delay between attempts. The record remains unacknowledged while retries are in progress.

After retry exhaustion, `KafkaRecordDeadLetterRecoverer` writes the following records in one SQLite transaction:

- The input payload and failure details to `dead_letter_event`.
- A terminal entry in `processed_input` with the `DEAD_LETTER` status.

After the recovery transaction commits, Spring Kafka commits the recovered record’s offset so that a permanently failing record does not block the partition indefinitely. If dead-letter persistence itself fails, the recovered offset is not considered successfully handled.


### Consumer Outcomes

| Input outcome | Retry behavior | Offset behavior |
|---|---|---|
| Successful processing | No retry | Manually acknowledged after durable processing |
| Late event | No retry | Persisted as `DROPPED_LATE`, then acknowledged |
| Malformed or incomplete record | No retry | Persisted to `dead_letter_event`, then acknowledged |
| Conflicting `click_id` | No retry | Persisted as a dead-letter conflict, then acknowledged |
| Unexpected processing failure | Retried with fixed backoff | Not acknowledged while retrying |
| Retry exhausted | Recovery replaces further retries | Dead-letter transaction commits before the recovered offset is committed |


### Startup Recovery

Kafka listeners do not start automatically. After the Spring Boot application is ready, the processor first restores:

- Maximum observed event times from `watermark_state`.
- Active clicks from `click_state`.
- Pending page views from `pending_page_view`.

Restored pending page views that are already behind the effective watermark are finalized before Kafka consumption begins.

If state restoration fails, the exception is propagated and the listeners are not started. This prevents new records from being processed with incomplete in-memory state.


### SQLite Write Failures

Normal click and page-view processing executes its durable changes in one SQLite transaction. This includes the relevant state changes, watermark update, processed-input record, and any final attribution output.

If a database operation fails:

- The transaction is rolled back.
- No staged in-memory changes are applied.
- The Kafka record is not acknowledged.
- Spring Kafka retries the record according to the configured error policy.

This behavior also applies when an output write fails or an existing `page_view_id` is found with conflicting output content.


### Process Restart

The recovery behavior depends on the point at which the previous process terminated:

| Failure point | Recovery behavior |
|---|---|
| Before SQLite commit | The transaction is rolled back and Kafka redelivers the unacknowledged record |
| After SQLite commit but before memory update | State is rebuilt from SQLite during startup |
| After memory update but before Kafka ACK | Kafka may redeliver the record; idempotency prevents duplicate output |
| After Kafka ACK | Consumption resumes from the committed consumer-group offset |

Final outputs, processed-input records, and dead letters remain durable in SQLite. Only active clicks, pending page views, and watermarks need to be rebuilt in memory.

Recovery assumes that the SQLite database file is stored on durable storage. Loss or corruption of that file cannot be reconstructed solely from already committed Kafka consumer-group offsets.


## VIII. Concurrency Model

### Kafka Partition Concurrency

The processor uses two Spring Kafka listener containers: one for `page_views` and one for `ad_clicks`. Each container uses the configured consumer concurrency, allowing records from different Kafka partitions to be processed by different listener threads.

The effective parallelism is bounded by:

- The configured listener concurrency.
- The number of partitions in each topic.
- The availability of the SQLite connection.

Because the two topics use separate listener containers, records from the same numeric partition in `page_views` and `ad_clicks` may arrive at the join engine concurrently. Additional application-level synchronization is therefore required.

### Partition-level Serialization

The join engine maintains one fair lock for each numeric partition.

The partition lock serializes all click, page-view, watermark, and pending-state changes associated with that partition, including events arriving from different input topics. This prevents concurrent events for the same partition from observing or producing inconsistent attribution state.

The lock covers:

- Reading the partition watermark.
- Executing the SQLite transaction.
- Calculating attribution results.
- Applying committed changes to the in-memory state.

Different partitions use different locks and can therefore continue processing concurrently.

This design relies on both input topics using the same partition count and the same `user_id`-based partitioning strategy. Under this assumption, all click and page-view events for one user are coordinated by the same numeric partition lock.

### Concurrent Click State

The in-memory click state uses the following logical structure:

```text
ConcurrentHashMap<userId, sorted clicks>
```

Each user’s clicks are stored in a `TreeSet` ordered by:

```text
event_time → click_id
```

All operations for a user are performed through atomic `ConcurrentHashMap` computations.

### Pending Page-view State

Pending page views use a partition-keyed concurrent map:

```text
ConcurrentHashMap<partition, sorted pending page views>
```

Within each partition, page views are ordered by:

```text
event_time → event_id
```

This ordering allows finalization to scan from the earliest pending page view and stop as soon as it reaches a page view beyond the effective watermark.

### Global Maintenance Coordination

A fair global read/write lock coordinates normal event processing with cross-partition maintenance.

Normal event processing acquires the read lock. Multiple partitions may hold this lock concurrently, preserving partition-level parallelism.

The write lock is used for operations that require a stable global view, including:

- Calculating the minimum watermark across partitions.
- Evicting expired click state.
- Migrating evicted clicks to click history.
- Migrating processed-input records to history.

While a maintenance operation holds the write lock, new event-processing operations wait. This prevents global state cleanup from interleaving with click, watermark, or pending-page-view updates.

The locking order is consistent:

```text
global state lock → partition lock
```

Using a fixed order avoids circular lock acquisition between normal processing and maintenance tasks.

### SQLite Write Serialization

Kafka consumption and in-memory processing support partition-level concurrency, but SQLite remains a single-writer database.

The configured Hikari pool contains one connection. Consequently, SQLite transactions from concurrent listener threads are serialized even when they belong to different Kafka partitions.

The WAL journal mode allows reads to continue while writes are committed, but it does not provide multiple concurrent SQLite writers. The single database connection therefore remains the primary limit on durable write throughput.


## IX. Result Verification

### Concurrency and Load-test Status

The implementation was verified through unit tests, in-process concurrency stress tests, and an end-to-end Kafka-to-SQLite workload. The tested scope passed without incorrect attribution, data loss, deadlock, database-lock errors, or dead-letter records.

### In-process Concurrency Stress Test

A dedicated Java harness executed 3,830,001 operations using 32 concurrent threads against the actual state-management and locking classes.

| Test area | Workload | Result |
|---|---:|---|
| Balanced click state | 500,000 inserts and 100,000 lookups | All inserts and lookups correct |
| Click eviction | 250,000 expected evictions | Exact result obtained |
| Hot-user state | 20,000 clicks for one user | Latest-click lookup correct |
| Pending page views | 200,000 concurrent inserts | Size and ordering correct |
| Watermarks | 2,000,000 updates across 64 partitions | No watermark mismatch |
| Global read/write lock | 1,000,000 reads and 10,000 writes | No deadlock or writer starvation |

The balanced click-state workload achieved approximately 1,026,762 operations per second. The pending-view buffer achieved approximately 1,658,776 insertions per second, while the watermark test achieved approximately 30.6 million updates per second.

The single-hot-user workload achieved approximately 14,665 operations per second. This was about 88.6 times slower than the balanced insertion workload, showing that highly skewed traffic concentrated on one user can become a contention and algorithmic-performance hotspot.

### End-to-end Kafka Test

The end-to-end test used:

- 12 partitions for the click topic
- 12 partitions for the page-view topic
- 12 click listener threads
- 12 page-view listener threads
- 24 Kafka consumer threads in total
- 260,000 input records
- 70,000 expected attribution outputs

The workload contained three scenarios:

| Scenario | Input records | Expected outputs | Result |
|---|---:|---:|---|
| Balanced traffic and restart recovery | 120,000 | 30,000 | All outputs correct |
| Out-of-order page views and clicks | 60,000 | 20,000 | All outputs correct |
| Duplicate click replay | 80,000 | 20,000 | All duplicates handled correctly |
| **Total** | **260,000** | **70,000** | **All validations passed** |

The final database state showed:

- 260,000 processed input records, all in `PROCESSED` state
- 70,000 output records
- 70,000 pending records, all finalized as `EMITTED`
- 0 active pending records
- 0 incorrect click attributions
- 0 dead-letter records
- 0 Kafka consumer lag across all tested partitions
- Successful SQLite integrity verification

### Ordering, Deduplication, and Recovery

The test verified that a page view arriving before its matching click was still attributed correctly when the click arrived within the configured allowed-lateness interval.

Duplicate click records were replayed at different Kafka offsets. The processor retained one logical click state and produced exactly one correct output for each logical page view.

The Java process was forcibly terminated after 90,000 records had been persisted, including 30,000 unresolved page views. After restart, the processor restored its watermarks, click state, pending state, and processed-input state from SQLite. Kafka listeners resumed in approximately 3.17 seconds, and all 30,000 pending page views were finalized correctly.

### Throughput and Resource Usage

Across the active end-to-end processing phases, the application processed 260,000 records in approximately 507 seconds.

- Aggregate throughput: approximately 512.8 records per second
- Per-stage throughput range: approximately 387.0 to 690.1 records per second
- Peak Java process memory: approximately 584 MiB RSS
- Final SQLite database, WAL, and shared-memory size: approximately 194 MiB

The producers intentionally generated records substantially faster than the application could consume them, creating Kafka backlog and ensuring that the processor remained under sustained load. All backlog was eventually consumed.

Although the Kafka listeners provided 24-way consumer concurrency, SQLite writes were effectively serialized because the Hikari connection pool was configured with a maximum size of one. Therefore, the observed throughput mainly reflects the single-writer SQLite persistence boundary rather than Kafka's maximum processing capacity.

### Verification Conclusion

The implementation passed the tested 100,000-plus-record concurrency and recovery workload. It preserved attribution correctness, ordering, deduplication, emit-once behavior, and recoverable state under balanced traffic, out-of-order delivery, duplicate replay, sustained backlog, and forced process restart.

The result demonstrates stable behavior under moderate single-instance concurrency. It is not a production-scale performance guarantee: long-duration soak tests, broker outages, Kafka consumer-group rebalances, disk-full conditions, database corruption, multi-instance deployment, and percentile-latency measurements were outside the tested scope.


## X. Capacity Planning and Scalability


### Capacity Limitations and SQLite Growth

Pending page views currently have no independent timeout or configured capacity limit. If an idle partition stops advancing its watermark, both its in-memory pending set and its durable `PENDING` rows may continue to grow.

Finalized page views are removed from the in-memory pending state, but their `EMITTED` rows currently remain in `pending_page_view`. A mechanism needs to be designed to delete it periodically.

The history tables are archivals. `click_history` and `processed_input_history` may grow without limit, requiring a detailed historical data processing strategy.

Appropriate external strategies are needed to control the size of the database.

### Horizontal Scaling and Multi-instance Deployment

Currently, horizontal scaling is not possible by simply launching additional application instances.

Future implementations could:

Migrate persistent state, etc., to a shared transactional database, or use partition-ownership-based processing and storage (e.g., using regions to divide multiple horizontal partitions). This would allow for coordinating and scaling the system across multiple instances at the database level.

### Storage Throughput

Since SQLite only allows one write at a time, simply increasing the connection pool size cannot achieve parallel writes. Consider:

- Batching multiple statements into fewer transactions to reduce transaction switching overhead

- Reducing transaction duration and separating read-intensive operations where appropriate

- Consider separating scheduled tasks that migrate tables to historical records, or running them only when traffic is low

- Partitioning by region, etc., to increase throughput through horizontal scaling

- Using a database that supports concurrent writes (e.g., PostgreSQL)

### Watermark and Idle-partition Handling

Possible improvements include:

- Detecting idle partitions.

- Advancing watermarks using a processing-time policy.

- Using partition-local eviction instead of a single global minimum watermark.


### State-management Efficiency

Potential improvements include:

- Maintaining a direct `click_id` index for constant-time duplicate detection.
- Maintaining time-based indexes for click eviction instead of scanning all users.
- Processing eviction and archival incrementally without holding the global maintenance lock for the full operation.


# ↓ original readme ↓


# Real-time Session Attribution with Windowed Stream Joins

## Welcome!

You have received your first challenge to become a part of an awesome team!

**A note about AI tools:** Using AI is not forbidden, but keep in mind this challenge is designed to understand your capabilities. The more you rely on AI, the less we'll understand your real code skills, problem-solving approach, and architectural thinking. We encourage you to use AI as a productivity tool, but make sure the core design decisions and implementation logic reflect your own understanding.

---

## Challenge Overview

Implement a **stream processor** that consumes two event streams and produces an output stream with joined data. This challenge will demonstrate your ability to handle **stream joins, real-time data, delivery guarantees, and concurrency** in a production-like streaming system.

### What You'll Build

A stream processing system that joins two event streams:

**Stream A: `page_view` events**
- `user_id`, `event_time`, `url`, `event_id`

**Stream B: `ad_click` events**
- `user_id`, `event_time`, `campaign_id`, `click_id`

**Goal:** For each `page_view`, attach the **most recent** `ad_click` for the same `user_id` that happened within **30 minutes** prior to `page_view` event in **event time**, and emit new `attributed_page_view` event with fields:

- `page_view_id`, `page_id`, `user_id`, `event_time`, `url`, `attributed_campaign_id` (nullable), `attributed_click_id` (nullable)

### Example Flow

```
User clicks ad for campaign X (12:00) → User views page (12:01) → Output: Page view attributed to ad campaign X
User views page (12:05) → No prior click for that user on that page → Output: Page view with nulls in `attributed_campaign_id` and `attributed_click_id`
```

---

## Important: Repository Naming

**To avoid this challenge being tracked or indexed online, you must name your repository as a car brand and model.**

Examples: `tesla-model-s`, `honda-civic`, `ford-mustang`, `toyota-camry`

**Do not** use terms like "challenge", "interview", "stream-processor", etc. in your repository name.

---

## Requirements

### 1) Stream Join + Out-of-Order Handling

* Events arrive **out of order** (with respect to `event_time`)
* You must support **watermarks**:
  * Configurable allowed lateness (max 15 minutes)
* A `page_view` can be emitted when you are confident no other `ad_click` will arrive that should supersede attribution (based on watermark), **or** you can emit updates to already produced `attributed_page_view` (but then specify your update strategy clearly).

**Edge cases to handle:**

* Click arrives after its page_view (but still within lateness)
* Multiple clicks in the 30-min window → pick the latest click
* Duplicate clicks in the 30-min window → emit only one attributed page view
* Late events beyond allowed lateness → drop or dead-letter (must be explicit)

### 2) Offset Management & Delivery Guarantees

* Input streams provide offsets (like Kafka):
  * `partition`, `offset`, `payload`
* You must implement a **consumer loop** that:
  * Consumes events from both streams
  * Implements join logic according to above spec
  * Writes output to a sink (local file or local DB) and commits consumed offsets to the stream
* Your implementation must be resilient to crashes and restarts. Document your delivery guarantees and what happens under different failure scenarios.

### 3) Concurrency

To achieve high throughput, your implementation should use multiple workers/threads to process partitions and/or batches concurrently. Make sure your implementation is thread-safe and works correctly under concurrent load.

### 5) Determinism + Testability

Provide a deterministic test harness:

* Feed a fixed set of events with known ordering and offsets
* Assert output records exactly (including attribution correctness)
* Include restart test: process half, crash, restart, ensure correctness

---

## Input/Output Contract

### Input

Two streams that yield records:

```
(topic, partition, offset, payload_json)
```

### Output

A sink interface:

```
write(record)
```

Offset commit interface:

```
commit(topic, partition, offset)
```
---

## Deliverables

1. **Working processor implementation** (language of choice: Java or Scala)
2. **Documentation explaining:**
   * Watermark logic
   * Write semantics (emit-once vs update)
   * Delivery guarantees (at-least-once, exactly-once, idempotence, etc.) and failure modes
   * Concurrency model
   * Capacity planning and scaling (state size, number of workers/instances)
3. **Tests for:**
   * Out-of-order events
   * Late data
   * Restart with committed offsets
   * Concurrent partitions
4. **README with:**
   * Setup instructions
   * How to run the processor
   * How to verify results

---

## Bonus Challenge

**Frontend Dashboard (optional, can be AI-generated)**

Create a simple web interface that shows the data flow in real-time:

* Visual representation of incoming events (clicks and page views)
* Current watermark position
* Attribution matches being made
* State size / memory usage
* Processing lag by partition

This can be built with any framework (React, Vue, simple HTML/JS) and can leverage AI code generation tools. The focus is on **visualizing the streaming concepts**, not frontend engineering excellence.

Example features:
* Live event stream visualization
* Attribution timeline view
* Metrics dashboard (throughput, latency, state size)
* Event inspector (click any event to see its journey)

---

## Event Schemas

### Input: Page View Event
```json
{
  "user_id": "user_1",
  "event_time": "2024-01-01T12:10:00Z",
  "url": "https://example.com/product",
  "event_id": "pv_1"
}
```

### Input: Ad Click Event
```json
{
  "user_id": "user_1",
  "event_time": "2024-01-01T12:00:00Z",
  "campaign_id": "campaign_A",
  "click_id": "click_1"
}
```

### Output: Attributed Page View
```json
{
  "page_view_id": "pv_1",
  "user_id": "user_1",
  "event_time": "2024-01-01T12:10:00Z",
  "url": "https://example.com/product",
  "attributed_campaign_id": "campaign_A",
  "attributed_click_id": "click_1"
}
```

## Test Scenarios

The data generator creates events covering these edge cases:

| Scenario | Description | Expected Behavior |
|----------|-------------|-------------------|
| **Normal** | Click at 12:00, page view at 12:10 | Attributes to campaign |
| **Out-of-order** | Page view arrives before its click | Handles via watermarks |
| **Multiple clicks** | 2+ clicks in 30-min window | Attributes to most recent |
| **Old click** | Click >30 minutes before page view | No attribution |
| **Late event** | Event beyond allowed lateness (2 min) | Dropped |
| **No click** | Page view with no prior click | Null attribution |

---

## Extension Ideas (Optional)

If you finish the core requirements and want to go further, consider these extensions:

* **Backpressure:** Simulate slow sink and ensure consumers don't OOM
* **Skew handling:** Hot users / hot partitions—how to mitigate contention
* **Metrics & Monitoring:** Add instrumentation to track processing latency, throughput, and state size

---

## Learning Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Kafka Reference](https://spring.io/projects/spring-kafka)
- [Streaming Systems Book](http://streamingsystems.net/)
- [The Dataflow Model Paper](https://research.google/pubs/pub43864/)

---

## Questions?

If you have questions about the requirements, edge cases, or technical setup, please reach out to your contact person.

---

**Good luck, and we're excited to see your solution! 🚀**
