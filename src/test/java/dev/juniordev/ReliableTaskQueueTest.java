package dev.juniordev;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ReliableTaskQueueTest {
    static final class MutableClock extends Clock {
        final AtomicLong time = new AtomicLong(1_000_000L);
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(time.get()); }
        @Override public long millis() { return time.get(); }
        void advance(long millis) { time.addAndGet(millis); }
    }

    private static ReliableTaskQueue.Config config() {
        return new ReliableTaskQueue.Config(3, 3, Duration.ofMillis(200),
                Duration.ofMillis(100), Duration.ofMillis(400), 100, 50);
    }

    private static ReliableTaskQueue queue(MutableClock clock) {
        return new ReliableTaskQueue(config(), clock);
    }

    private static ReliableTaskQueue.Request request(String id, String payload,
                                                      int priority, long delay) {
        return new ReliableTaskQueue.Request(id, payload, priority,
                Duration.ofMillis(delay), null);
    }

    @Test
    void defaultsAreUsableAndCountersStartAtZero() {
        ReliableTaskQueue q = new ReliableTaskQueue();
        assertEquals(1_000, q.config().capacity());
        assertEquals(0, q.stats().active());
        assertEquals(0, q.stats().terminal());
        assertTrue(q.claim().isEmpty());
        assertEquals(List.of(), q.history());
    }

    @Test
    void priorityIsDescendingAndFifoWithinEqualPriority() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        assertTrue(q.submit(request("low", "a", -2, 0)));
        assertTrue(q.submit(request("high1", "b", 5, 0)));
        assertTrue(q.submit(request("high2", "c", 5, 0)));
        assertEquals("high1", q.claim().orElseThrow().id());
        assertEquals("high2", q.claim().orElseThrow().id());
        assertEquals("low", q.claim().orElseThrow().id());
        assertTrue(q.claim().isEmpty());
        assertEquals(3, q.stats().leased());
        assertEquals(3, q.stats().deliveries());
    }

    @Test
    void delayedJobsBecomeAvailableOnlyAfterTheirDeadline() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(request("later", "later", 10, 200));
        q.submit(request("now", "now", 0, 0));
        assertEquals("now", q.claim().orElseThrow().id());
        assertTrue(q.claim().isEmpty());
        clock.advance(199);
        assertTrue(q.claim().isEmpty());
        clock.advance(1);
        assertEquals("later", q.claim().orElseThrow().id());
    }

    @Test
    void deduplicationProtectsActiveAndTerminalIds() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        assertTrue(q.submit(ReliableTaskQueue.Request.immediate("once", "first")));
        assertFalse(q.submit(ReliableTaskQueue.Request.immediate("once", "second")));
        var lease = q.claim().orElseThrow();
        assertTrue(q.acknowledge("once", lease.token()));
        assertFalse(q.submit(ReliableTaskQueue.Request.immediate("once", "third")));
        assertEquals("first", q.inspect("once").orElseThrow().payload());
        assertEquals(1, q.stats().submissions());
    }

    @Test
    void boundedCapacityCountsOnlyActiveJobs() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        for (int i = 0; i < 3; i++) {
            q.submit(ReliableTaskQueue.Request.immediate("job" + i, "payload"));
        }
        assertThrows(IllegalStateException.class,
                () -> q.submit(ReliableTaskQueue.Request.immediate("overflow", "x")));
        var lease = q.claim().orElseThrow();
        assertTrue(q.acknowledge(lease.id(), lease.token()));
        assertTrue(q.submit(ReliableTaskQueue.Request.immediate("next", "x")));
        assertEquals(3, q.stats().active());
        assertEquals(1, q.stats().succeeded());
    }

    @Test
    void rejectsInvalidRequestFieldsWithoutPartialSubmission() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        assertThrows(IllegalArgumentException.class,
                () -> q.submit(request("bad space", "x", 0, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> q.submit(request("ok", " ", 0, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> q.submit(request("ok", "x", 101, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> q.submit(request("ok", "x", -101, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> q.submit(request("ok", "x".repeat(101), 0, 0)));
        assertThrows(NullPointerException.class, () -> q.submit(null));
        assertEquals(0, q.stats().submissions());
    }

    @Test
    void rejectsInvalidConfigAndRequestDurations() {
        assertThrows(IllegalArgumentException.class,
                () -> new ReliableTaskQueue.Config(0, 3, Duration.ofSeconds(1),
                        Duration.ZERO, Duration.ofSeconds(1), 100, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new ReliableTaskQueue.Config(1, 0, Duration.ofSeconds(1),
                        Duration.ZERO, Duration.ofSeconds(1), 100, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new ReliableTaskQueue.Config(1, 1, Duration.ZERO,
                        Duration.ZERO, Duration.ofSeconds(1), 100, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new ReliableTaskQueue.Config(1, 1, Duration.ofSeconds(1),
                        Duration.ofSeconds(2), Duration.ofSeconds(1), 100, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new ReliableTaskQueue.Request("x", "y", 0, Duration.ofMillis(-1), null));
        assertThrows(IllegalArgumentException.class,
                () -> new ReliableTaskQueue.Request("x", "y", 0, Duration.ZERO, Duration.ZERO));
    }

    @Test
    void retryBackoffDoublesAndEndsInDeadLetter() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(ReliableTaskQueue.Request.immediate("retry", "payload"));
        var first = q.claim().orElseThrow();
        assertEquals(1, first.attempt());
        assertEquals(ReliableTaskQueue.FailureOutcome.RETRY_SCHEDULED,
                q.fail("retry", first.token(), "network"));
        assertTrue(q.claim().isEmpty());
        clock.advance(100);
        var second = q.claim().orElseThrow();
        assertEquals(2, second.attempt());
        assertEquals(ReliableTaskQueue.FailureOutcome.RETRY_SCHEDULED,
                q.fail("retry", second.token(), "network"));
        clock.advance(199);
        assertTrue(q.claim().isEmpty());
        clock.advance(1);
        var third = q.claim().orElseThrow();
        assertEquals(3, third.attempt());
        assertEquals(ReliableTaskQueue.FailureOutcome.DEAD_LETTER,
                q.fail("retry", third.token(), "permanent"));
        assertEquals(ReliableTaskQueue.State.DEAD, q.inspect("retry").orElseThrow().state());
        assertEquals(2, q.stats().retries());
        assertEquals(3, q.stats().deliveries());
        assertEquals("permanent", q.inspect("retry").orElseThrow().lastError());
    }

    @Test
    void leaseTimeoutReschedulesWithNewTokenAndRejectsStaleAck() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(ReliableTaskQueue.Request.immediate("timeout", "payload"));
        var old = q.claim().orElseThrow();
        clock.advance(200);
        assertEquals(1, q.sweep());
        assertFalse(q.acknowledge("timeout", old.token()));
        assertEquals(1, q.stats().leaseTimeouts());
        clock.advance(100);
        var replacement = q.claim().orElseThrow();
        assertNotEquals(old.token(), replacement.token());
        assertEquals(2, replacement.attempt());
        assertTrue(q.acknowledge("timeout", replacement.token()));
        assertFalse(q.acknowledge("timeout", replacement.token()));
    }

    @Test
    void repeatedVisibilityTimeoutEventuallyDeadLetters() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(ReliableTaskQueue.Request.immediate("stuck", "payload"));
        for (int attempt = 1; attempt <= 3; attempt++) {
            assertEquals(attempt, q.claim().orElseThrow().attempt());
            clock.advance(200);
            assertEquals(1, q.sweep());
            if (attempt < 3) {
                clock.advance(attempt == 1 ? 100 : 200);
            }
        }
        assertEquals(ReliableTaskQueue.State.DEAD, q.inspect("stuck").orElseThrow().state());
        assertEquals(3, q.stats().leaseTimeouts());
        assertEquals(2, q.stats().retries());
    }

    @Test
    void extensionProtectsRunningLease() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(ReliableTaskQueue.Request.immediate("long", "payload"));
        var lease = q.claim().orElseThrow();
        clock.advance(150);
        assertTrue(q.extendLease("long", lease.token(), Duration.ofMillis(300)));
        clock.advance(100);
        assertEquals(0, q.sweep());
        assertTrue(q.acknowledge("long", lease.token()));
        assertFalse(q.extendLease("long", lease.token(), Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
                () -> q.extendLease("long", lease.token(), Duration.ZERO));
    }

    @Test
    void cancellationInvalidatesInFlightToken() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(ReliableTaskQueue.Request.immediate("cancel", "payload"));
        var lease = q.claim().orElseThrow();
        assertTrue(q.cancel("cancel"));
        assertFalse(q.cancel("cancel"));
        assertFalse(q.acknowledge("cancel", lease.token()));
        assertEquals(ReliableTaskQueue.FailureOutcome.STALE,
                q.fail("cancel", lease.token(), "late"));
        assertEquals(1, q.stats().cancelled());
    }

    @Test
    void deadLetterReplayResetsAttemptsAndAllowsSuccess() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = new ReliableTaskQueue(
                new ReliableTaskQueue.Config(1, 1, Duration.ofMillis(100),
                        Duration.ZERO, Duration.ZERO, 100, 50), clock);
        q.submit(ReliableTaskQueue.Request.immediate("again", "payload"));
        var first = q.claim().orElseThrow();
        assertEquals(ReliableTaskQueue.FailureOutcome.DEAD_LETTER,
                q.fail("again", first.token(), "bad"));
        assertTrue(q.replayDeadLetter("again"));
        assertFalse(q.replayDeadLetter("again"));
        var second = q.claim().orElseThrow();
        assertEquals(1, second.attempt());
        assertTrue(q.acknowledge("again", second.token()));
        assertEquals(1, q.stats().succeeded());
    }

    @Test
    void ttlExpiresReadyJobBeforeClaim() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(new ReliableTaskQueue.Request("expires", "payload", 0,
                Duration.ZERO, Duration.ofMillis(50)));
        clock.advance(50);
        assertTrue(q.claim().isEmpty());
        assertEquals(1, q.stats().expired());
        assertEquals(0, q.stats().deliveries());
    }

    @Test
    void ttlExpiresLeasedJobAndInvalidatesToken() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(new ReliableTaskQueue.Request("lease-ttl", "payload", 0,
                Duration.ZERO, Duration.ofMillis(100)));
        var lease = q.claim().orElseThrow();
        clock.advance(100);
        assertFalse(q.acknowledge("lease-ttl", lease.token()));
        assertEquals(ReliableTaskQueue.State.EXPIRED,
                q.inspect("lease-ttl").orElseThrow().state());
        assertEquals(0, q.stats().leaseTimeouts());
    }

    @Test
    void retryCannotOutliveTtl() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(new ReliableTaskQueue.Request("ttl-retry", "payload", 0,
                Duration.ZERO, Duration.ofMillis(50)));
        var lease = q.claim().orElseThrow();
        assertEquals(ReliableTaskQueue.FailureOutcome.EXPIRED,
                q.fail("ttl-retry", lease.token(), "try again"));
        assertEquals(1, q.stats().expired());
    }

    @Test
    void delayCannotEqualOrExceedTtl() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        assertThrows(IllegalArgumentException.class,
                () -> q.submit(new ReliableTaskQueue.Request("invalid", "x", 0,
                        Duration.ofMillis(100), Duration.ofMillis(100))));
        assertEquals(0, q.stats().active());
    }

    @Test
    void historyIsBoundedAndSnapshotsCannotBeMutated() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = new ReliableTaskQueue(
                new ReliableTaskQueue.Config(10, 2, Duration.ofMillis(100),
                        Duration.ZERO, Duration.ZERO, 100, 2), clock);
        q.submit(ReliableTaskQueue.Request.immediate("a", "a"));
        q.submit(ReliableTaskQueue.Request.immediate("b", "b"));
        var lease = q.claim().orElseThrow();
        q.acknowledge(lease.id(), lease.token());
        assertEquals(2, q.history().size());
        assertThrows(UnsupportedOperationException.class, () -> q.history().clear());
        assertThrows(UnsupportedOperationException.class, () -> q.list().clear());
        assertEquals(1, q.history(lease.id()).size());
        assertEquals(2, q.list().size());
    }

    @Test
    void terminalRecordsCanBePurgedAndIdsReused() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(ReliableTaskQueue.Request.immediate("purge", "first"));
        var lease = q.claim().orElseThrow();
        assertTrue(q.acknowledge("purge", lease.token()));
        assertEquals(0, q.purgeTerminal(Duration.ofSeconds(1)));
        clock.advance(1_000);
        assertEquals(1, q.purgeTerminal(Duration.ofSeconds(1)));
        assertTrue(q.inspect("purge").isEmpty());
        assertTrue(q.submit(ReliableTaskQueue.Request.immediate("purge", "second")));
        assertEquals("second", q.inspect("purge").orElseThrow().payload());
    }

    @Test
    void errorMessagesAreSanitizedAndTruncated() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(ReliableTaskQueue.Request.immediate("sanitize", "payload"));
        var lease = q.claim().orElseThrow();
        q.fail("sanitize", lease.token(), "bad\ninput\t" + "z".repeat(300));
        String error = q.inspect("sanitize").orElseThrow().lastError();
        assertEquals(160, error.length());
        assertFalse(error.contains("\n"));
        assertFalse(error.contains("\t"));
    }

    @Test
    void batchClaimsAtMostAvailableAndRejectsInvalidLimit() {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(ReliableTaskQueue.Request.immediate("one", "a"));
        q.submit(ReliableTaskQueue.Request.immediate("two", "b"));
        assertEquals(2, q.claimBatch(3).size());
        assertTrue(q.claimBatch(1).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> q.claimBatch(0));
        assertThrows(IllegalArgumentException.class, () -> q.claimBatch(1001));
    }

    @Test
    void concurrentClaimsNeverDeliverTheSameJobTwice() throws Exception {
        MutableClock clock = new MutableClock();
        ReliableTaskQueue q = queue(clock);
        q.submit(ReliableTaskQueue.Request.immediate("unique", "payload"));
        var executor = Executors.newFixedThreadPool(8);
        CountDownLatch ready = new CountDownLatch(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<Integer>>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    start.await(5, TimeUnit.SECONDS);
                    return q.claim().isPresent() ? 1 : 0;
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            int delivered = 0;
            for (var future : futures) {
                delivered += future.get(5, TimeUnit.SECONDS);
            }
            assertEquals(1, delivered);
            assertEquals(1, q.stats().deliveries());
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
