# Conditional Cache Example

This example demonstrates the explicit conditional replacement API of
`com.github.benmanes.caffeine.cache.ConditionalCache`.

## Overview

A read returns a `Snapshot` pairing the current value with a generation token. An update
commits only when the token still belongs to the current value, providing an optimistic
concurrency control (compare-and-swap) discipline on top of a regular cache.

## Key points

- **Read returns value + token**: `getIfPresent(key)` returns a `Snapshot<V>` with both the
  value and its generation token.
- **Conditional replace**: `replace(key, expectedToken, newValue)` commits only if the
  generation still matches. Returns `true` on commit, `false` on rejection.
- **Failure cleanup**: a rejected candidate is handed to an independent `failureCleanup`
  callback exactly once. A successful commit never invokes the callback.
- **Compute**: `compute(key, transform, failureCleanup)` reads the current value, applies the
  transform *outside* the cache's internal lock (so it can safely reenter the cache), and
  re-checks the generation at commit time.
- **Expiration safety**: after an entry expires and is reloaded, the old token is rejected
  because the reloaded value gets a fresh generation.
- **Ordinary get/put**: behave as they would on a normal cache.

## Running

```bash
./gradlew run
```

Or run the tests:

```bash
./gradlew test
```

## JUnit command

To run the conditional cache tests with Gradle:

```bash
./gradlew :caffeine:test --tests "com.github.benmanes.caffeine.cache.ConditionalCacheTest"
```

To run the example tests:

```bash
cd examples/conditional-cache
./gradlew test
```
