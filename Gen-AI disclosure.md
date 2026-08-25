# Generative AI Usage Disclosure

Generative AI (CodeX) was used as an auxiliary tool in the completion of this project. The extent of the use of generative AI in each task is disclosed as follows:

## AI Usage Level Reference

Manual participation (approximately) : 100% = M1 > M2 > M3 > C1 = 50% > A1 > A2 = 20%

| Level | Description |
|---|---|
| M1 | Completed entirely through manual work. |
| M2 | Completed primarily through manual work, with AI providing assistance. |
| M3 | Design determined manually; AI supplemented parts of the design, with manual review. |
| C1 | Design determined collaboratively through manual work and AI; content completed primarily by AI, with manual review. |
| A1 | AI completed the primary content based on manual instructions or reference materials, with manual review. |
| A2 | Completed entirely by AI, with manual review. |

## Part 1: Java Source Code

| Package | File | AI Usage Level |
|---|---|----------------|
| `com.ebay.challenge.streamprocessor.consumer` | `KafkaRecordDeadLetterRecoverer.java` | M2             |
| `com.ebay.challenge.streamprocessor.consumer` | `StreamConsumer.java` | M1             |
| `com.ebay.challenge.streamprocessor.consumer` | `StreamConsumerStarter.java` | M2             |
| `com.ebay.challenge.streamprocessor.engine` | `JoinEngine.java` | M1             |
| `com.ebay.challenge.streamprocessor.infrastructure` | `InvalidKafkaRecordException.java` | M2             |
| `com.ebay.challenge.streamprocessor.infrastructure` | `OutputSinkException.java` | M1             |
| `com.ebay.challenge.streamprocessor.infrastructure` | `StateAccessLock.java` | M2             |
| `com.ebay.challenge.streamprocessor.job` | `ClickHistoryMigrationJob.java` | M2             |
| `com.ebay.challenge.streamprocessor.job` | `ProcessedInputHistoryMigrationJob.java` | A1             |
| `com.ebay.challenge.streamprocessor.mapper` | `ClickHistoryMapper.java` | C1             |
| `com.ebay.challenge.streamprocessor.mapper` | `ClickStateMapper.java` | C1             |
| `com.ebay.challenge.streamprocessor.mapper` | `DeadLetterEventMapper.java` | C1             |
| `com.ebay.challenge.streamprocessor.mapper` | `OutputMapper.java` | C1             |
| `com.ebay.challenge.streamprocessor.mapper` | `PendingPageViewMapper.java` | C1             |
| `com.ebay.challenge.streamprocessor.mapper` | `ProcessedInputHistoryMapper.java` | C1             |
| `com.ebay.challenge.streamprocessor.mapper` | `ProcessedInputMapper.java` | C1             |
| `com.ebay.challenge.streamprocessor.mapper` | `WatermarkStateMapper.java` | C1             |
| `com.ebay.challenge.streamprocessor.model` | `AdClickEvent.java` | M1             |
| `com.ebay.challenge.streamprocessor.model` | `AttributedPageView.java` | M1             |
| `com.ebay.challenge.streamprocessor.model` | `MemoryStateChanges.java` | M3             |
| `com.ebay.challenge.streamprocessor.model` | `PageViewEvent.java` | M1             |
| `com.ebay.challenge.streamprocessor.model` | `PendingPageview.java` | M1             |
| `com.ebay.challenge.streamprocessor.output` | `OutputSink.java` | M2             |
| `com.ebay.challenge.streamprocessor.state` | `ClickStateStore.java` | M1             |
| `com.ebay.challenge.streamprocessor.state` | `WatermarkTracker.java` | M1             |

## Part 2: Functional Modules

| Functional Module | AI Usage Level |
|---|----------------|
| Core Attribution Business Logic | M1             |
| Watermark and Out-of-Order Processing | M1             |
| Kafka Consumption and Input Validation | M2             |
| Delivery Guarantees and Error Handling | M3             |
| State Management and Persistence | M2             |
| Startup and Failure Recovery | M2             |
| Concurrency Control | M1             |
| State Eviction and History Archival | M2             |
| Application Configuration | A1             |

## Part 3: `README_.md`

| Section | AI Usage Level |
|---|----------------|
| I. Project Overview and Implementation Scope | A1             |
| II. Evironment Setup and Start | A2             |
| III. Configuration Reference | A2             |
| IV. Join Semantics | C1             |
| V. Watermark Logic | C1             |
| VI. State Model, Eviction, and Archival | C1             |
| VII. Output and Delivery Semantics | C1             |
| VIII. Concurrency Model | C1             |
| IX. Result Verification | A2             |
| X. Capacity Planning and Scalability | M1             |

## Part 4: Java Tests

| Package | File | AI Usage Level |
|---|---|----------------|
| `com.ebay.challenge.streamprocessor.config` | `DatabaseConfigTest.java` | A1             |
| `com.ebay.challenge.streamprocessor.config` | `KafkaConsumerConfigTest.java` | A1             |
| `com.ebay.challenge.streamprocessor.consumer` | `KafkaRecordDeadLetterRecovererTest.java` | A1             |
| `com.ebay.challenge.streamprocessor.consumer` | `StreamConsumerStarterTest.java` | A1             |
| `com.ebay.challenge.streamprocessor.consumer` | `StreamConsumerTest.java` | M3             |
| `com.ebay.challenge.streamprocessor.engine` | `JoinEngineTest.java` | M2             |
| `com.ebay.challenge.streamprocessor.infrastructure` | `StateAccessLockTest.java` | A1             |
| `com.ebay.challenge.streamprocessor.job` | `ClickHistoryMigrationJobTest.java` | A1             |
| `com.ebay.challenge.streamprocessor.job` | `ProcessedInputHistoryMigrationJobTest.java` | A1             |
| `com.ebay.challenge.streamprocessor.mapper` | `ClickHistoryMapperTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.mapper` | `ClickStateMapperTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.mapper` | `DeadLetterEventMapperTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.mapper` | `MapperTestSupport.java` | A2             |
| `com.ebay.challenge.streamprocessor.mapper` | `OutputMapperTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.mapper` | `PendingPageViewMapperTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.mapper` | `ProcessedInputHistoryMapperTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.mapper` | `ProcessedInputMapperTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.mapper` | `WatermarkStateMapperTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.model` | `EventModelJsonTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.model` | `MemoryStateChangesTest.java` | A2             |
| `com.ebay.challenge.streamprocessor.model` | `PendingPageviewTest.java` | M2             |
| `com.ebay.challenge.streamprocessor.output` | `OutputSinkTest.java` | M3             |
| `com.ebay.challenge.streamprocessor.state` | `ClickStateStoreTest.java` | M2             |
| `com.ebay.challenge.streamprocessor.state` | `WatermarkTrackerTest.java` | M2             |
