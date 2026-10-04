# Eviction event collector

A composable layer that sits on top of a Caffeine cache and hands
[`RemovalListener`][removal-listener] events to a caller in **bounded, immutable batches** with
at-least-once delivery. The collector only *observes* the events the cache already emits; it does
not modify the cache's eviction algorithm.

## Why

Caffeine's `RemovalListener` is invoked on a cache thread, so a listener that does blocking work
(durably writing evictions to a downstream system, acknowledging them to a message broker, ...)
must not run inline. This collector decouples the cache thread from the caller's processing pace:

- the cache thread never blocks;
- the caller processes batches at its own pace;
- a failed batch is re-fetched unchanged;
- capacity is released only after explicit acknowledgment;
- if the caller falls behind, excess events are dropped and counted rather than queued without
  bound.

## Usage

```java
// Bounded collection space: at most 64 events held across all unacknowledged batches.
EvictionEventCollector<Key, Value> collector = EvictionEventCollector.create(64);

Cache<Key, Value> cache = Caffeine.newBuilder()
    .maximumSize(10_000)
    .removalListener(collector)
    .build();

// Consumer loop (run on the caller's own executor, never on a cache thread):
for (;;) {
  EvictionBatch<Key, Value> batch = collector.pollBatch();
  if (batch == null) {
    // No batch available yet; poll again later.
    break;
  }
  try {
    downstream.write(batch.events());   // process the immutable snapshot
    collector.acknowledge(batch.batchNumber());
  } catch (RuntimeException failure) {
    // Do NOT acknowledge: the next poll re-fetches the same batch with the same number,
    // same events, and same order.
  }
}
```

To compose with an existing listener, register it as a delegate:

```java
EvictionEventCollector<Key, Value> collector = EvictionEventCollector.<Key, Value>builder()
    .maxCapacity(64)
    .delegate(existingListener)
    .build();
```

## Semantics

### Batching and acknowledgment

- Events accumulate into the **current batch**.
- `pollBatch()` seals the current batch and returns an immutable `EvictionBatch` carrying a
  monotonically increasing batch number (starting at `1`).
- A polled batch is **outstanding**: its events remain held and its capacity is reserved until
  acknowledged.
- If a batch is outstanding, `pollBatch()` (and `fetchBatch(number)`) re-fetches that same batch:
  the returned snapshot has the same batch number, the same events, and the same order. The next
  batch only becomes available after the outstanding one is acknowledged.
- `acknowledge(number)` releases the batch's capacity. It is **idempotent**: acknowledging the same
  batch more than once has no additional effect and never releases capacity twice. Concurrent
  acknowledgment of the same batch releases it exactly once.

### Bounded space and drop semantics

- The collector holds at most `maxCapacity` events across the current batch and all outstanding
  batches.
- When an event arrives and the collector is **full**, the event is **dropped**: the
  [drop count](#monitoring) is incremented **synchronously on the calling cache thread**, without
  blocking. This preserves the cache thread's liveness at the cost of at-most-once delivery for the
  dropped events.
- Explicit removal (`RemovalCause.EXPLICIT`) and capacity eviction (`RemovalCause.SIZE`) events are
  counted and dropped alike. The original cause is preserved for every event that is delivered.
- Dropping is a deliberate backpressure choice: the collector never blocks the cache thread and
  never grows without bound. If dropped events are unacceptable, size `maxCapacity` for the expected
  in-flight volume and alert on a non-zero drop count.

### Immutable snapshots

- `EvictionBatch` and `EvictionEvent` are immutable value types. The event list is an unmodifiable
  `List.copyOf` snapshot; callers cannot mutate it.
- Re-fetching a batch returns the same snapshot instance, so retries observe identical content.

### Monitoring

| Method | Meaning |
| --- | --- |
| `droppedCount()` | total events dropped because the collector was full |
| `acknowledgedCount()` | total batches acknowledged |
| `pendingEventCount()` | events currently held (current + outstanding batches) |
| `pendingBatchCount()` | outstanding (dispatched but not yet acknowledged) batches |
| `currentBatchSize()` | events in the current, not-yet-dispatched batch |
| `maxCapacity()` | the configured bound on held events |

## Run the demo

```bash
./gradlew run
```

The demo fills a size-2 cache with a size-2 collector: the first two evictions are held, the rest
are dropped; it then re-fetches the first batch (same snapshot), acknowledges it, and observes the
next batch appearing.

## Run the tests

```bash
./gradlew test
```

The tests cover: re-fetch consistency with a direct executor and a size-2 cache (three removals:
one capacity eviction and two explicit removals), idempotent acknowledgment, concurrent
acknowledgment with no double release, bounded-space drop counting, and immutable snapshots.

[removal-listener]: https://javadoc.io/doc/com.github.ben-manes.caffeine/caffeine/latest/com/github/benmanes/caffeine/cache/RemovalListener.html
