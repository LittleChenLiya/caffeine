# Batched Removal Collector

A composable eviction-event collection layer that sits **outside** Caffeine. It is a plain
`RemovalListener` decorator: the cache's eviction algorithm is not modified in any way — the
collector only observes the removal events the cache already publishes and hands them to the
caller in acknowledged batches. The original `RemovalCause` of every event is preserved, so
explicit invalidations (`EXPLICIT`) stay distinguishable from capacity evictions (`SIZE`).

## Usage

```java
var collector = BatchedRemovalCollector.<String, String>create(1_024);

Cache<String, String> cache = Caffeine.newBuilder()
    .maximumSize(10_000)
    .executor(Runnable::run)      // publish events synchronously (any executor works)
    .removalListener(collector)   // the composable collection layer
    .build();

// Consumer side: poll, process, confirm.
collector.pollBatch(100).ifPresent(batch -> {
  try {
    writeToDownstream(batch.events()); // immutable snapshot, original order
    collector.confirm(batch.id());     // release the batch capacity
  } catch (RuntimeException e) {
    // Leave the batch unconfirmed: collector.pendingBatch() re-fetches the same
    // batch (same id, same events, same order) for a retry on the next pass.
  }
});
```

## Batch protocol

- `pollBatch(maxEvents)` forms a batch from the oldest buffered events and returns an
  immutable snapshot (`List.copyOf`) with a monotonically increasing batch id (1, 2, 3, ...).
  At most one batch is outstanding at a time: while a batch is unconfirmed, `pollBatch`
  returns `Optional.empty()` and the next batch only appears after `confirm`.
- `pendingBatch()` re-fetches the outstanding batch. After a processing failure the caller
  retries with the identical batch: same id, same events, same order.
- `confirm(batchId)` releases the buffer capacity held by the batch. It is idempotent:
  confirming the same id twice, confirming a stale id, or racing concurrent confirms all
  release the capacity exactly once; the losing calls return `false`.

## Drop semantics

The collection space is bounded by the fixed capacity passed to `create(int)`. Capacity is
held by every buffered event that has not been confirmed yet — including the events of an
unconfirmed batch — and is released only by `confirm`.

When the buffer is full, `onRemoval`:

- **never blocks the cache thread** — there is no waiting for consumer progress;
- **never evicts an already accepted event** — accepted events are only released by `confirm`;
- **discards the incoming event** and records the drop **synchronously** in the calling
  thread, incrementing the counter exposed by `dropped()`.

Drops are therefore lossy but always observable: a caller that sees `dropped() > 0` knows
exactly how many events were discarded and can react (for example by polling and confirming
more promptly, or by increasing the capacity). This mirrors Caffeine's own best-effort
philosophy for its internal buffers: the cache's hot path is never stalled by a slow or
stalled consumer.

## Running the tests

From the repository root (the example is a standalone Gradle build):

```bash
./gradlew -p examples/batched-removal test
```
