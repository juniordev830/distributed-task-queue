package dev.juniordev;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TaskQueueWorkerReportsTest {
    private ReliableTaskQueue queue(int attempts) {
        return new ReliableTaskQueue(new ReliableTaskQueue.Config(
                50, attempts, Duration.ofSeconds(5), Duration.ZERO,
                Duration.ZERO, 500, 100), Clock.systemUTC());
    }

    @Test
    void emptyQueueReturnsNoJobWithoutCallingHandler() {
        ReliableTaskQueue q = queue(2);
        AtomicInteger invoked = new AtomicInteger();
        var attempt = TaskQueueWorker.processOne(q, payload -> invoked.incrementAndGet());
        assertEquals(TaskQueueWorker.Outcome.NO_JOB, attempt.outcome());
        assertEquals("", attempt.id());
        assertEquals(0, invoked.get());
    }

    @Test
    void successfulHandlerAcknowledgesAndUpdatesStats() {
        ReliableTaskQueue q = queue(2);
        q.submit(ReliableTaskQueue.Request.immediate("success", "hello"));
        AtomicInteger calls = new AtomicInteger();
        var attempt = TaskQueueWorker.processOne(q, payload -> {
            assertEquals("hello", payload);
            calls.incrementAndGet();
        });
        assertTrue(attempt.succeeded());
        assertFalse(attempt.failed());
        assertEquals(1, calls.get());
        assertEquals(1, q.stats().succeeded());
        assertEquals(0, q.stats().active());
    }

    @Test
    void checkedExceptionSchedulesRetryThenDeadLetter() {
        ReliableTaskQueue q = queue(2);
        q.submit(ReliableTaskQueue.Request.immediate("fail", "payload"));
        TaskQueueWorker.Handler broken = payload -> {
            throw new java.io.IOException("temporarily unavailable");
        };
        var first = TaskQueueWorker.processOne(q, broken);
        assertEquals(TaskQueueWorker.Outcome.RETRY_SCHEDULED, first.outcome());
        assertTrue(first.failed());
        assertTrue(first.error().contains("IOException"));
        var second = TaskQueueWorker.processOne(q, broken);
        assertEquals(TaskQueueWorker.Outcome.DEAD_LETTER, second.outcome());
        assertEquals(1, q.stats().dead());
        assertEquals(2, q.stats().deliveries());
    }

    @Test
    void workerDoesNotSwallowAssertionErrors() {
        ReliableTaskQueue q = queue(2);
        q.submit(ReliableTaskQueue.Request.immediate("assertion", "payload"));
        assertThrows(AssertionError.class,
                () -> TaskQueueWorker.processOne(q, payload -> {
                    throw new AssertionError("programming bug");
                }));
        assertEquals(1, q.stats().leased());
    }

    @Test
    void drainHonorsMaximumAndStopsWhenQueueIsEmpty() {
        ReliableTaskQueue q = queue(2);
        for (int i = 0; i < 4; i++) {
            q.submit(ReliableTaskQueue.Request.immediate("job" + i, "payload" + i));
        }
        var first = TaskQueueWorker.drain(q, payload -> {}, 2);
        assertEquals(2, first.size());
        assertEquals(2, first.successes());
        assertEquals(0, first.failures());
        var second = TaskQueueWorker.drain(q, payload -> {}, 10);
        assertEquals(2, second.size());
        assertEquals(4, q.stats().succeeded());
        assertThrows(IllegalArgumentException.class,
                () -> TaskQueueWorker.drain(q, payload -> {}, 0));
    }

    @Test
    void parallelRunnerProcessesEachJobOnce() throws Exception {
        ReliableTaskQueue q = queue(2);
        for (int i = 0; i < 12; i++) {
            q.submit(ReliableTaskQueue.Request.immediate("p" + i, "payload" + i));
        }
        AtomicInteger processed = new AtomicInteger();
        var executor = Executors.newFixedThreadPool(4);
        try {
            var batch = TaskQueueWorker.parallel(q,
                    payload -> processed.incrementAndGet(), executor, 8);
            assertEquals(8, batch.size());
            assertEquals(8, batch.successes());
            assertEquals(8, processed.get());
            assertEquals(4, q.stats().ready());
            assertEquals(8, q.stats().succeeded());
            assertThrows(IllegalArgumentException.class,
                    () -> TaskQueueWorker.parallel(q, payload -> {}, executor, 0));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void handlerCanSubmitWhileAnotherHandlerIsRunning() throws Exception {
        ReliableTaskQueue q = queue(2);
        q.submit(ReliableTaskQueue.Request.immediate("slow", "payload"));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var processing = executor.submit(() -> TaskQueueWorker.processOne(q, payload -> {
                started.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
            }));
            assertTrue(started.await(5, TimeUnit.SECONDS));
            var submission = executor.submit(() ->
                    q.submit(ReliableTaskQueue.Request.immediate("other", "payload")));
            assertTrue(submission.get(5, TimeUnit.SECONDS));
            release.countDown();
            assertEquals(TaskQueueWorker.Outcome.SUCCEEDED,
                    processing.get(5, TimeUnit.SECONDS).outcome());
            assertEquals(1, q.stats().ready());
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancelledJobProducesStaleAcknowledgement() {
        ReliableTaskQueue q = queue(2);
        q.submit(ReliableTaskQueue.Request.immediate("cancelled", "payload"));
        var attempt = TaskQueueWorker.processOne(q, payload ->
                assertTrue(q.cancel("cancelled")));
        assertEquals(TaskQueueWorker.Outcome.STALE, attempt.outcome());
        assertEquals(1, q.stats().cancelled());
    }

    @Test
    void csvEscapesFormulasQuotesAndNewlines() {
        ReliableTaskQueue q = queue(2);
        q.submit(ReliableTaskQueue.Request.immediate("formula", "=SUM(1,2)"));
        q.submit(ReliableTaskQueue.Request.immediate("quoted", "a\"b\nc"));
        String csv = TaskQueueReports.jobsCsv(q);
        assertTrue(csv.startsWith("id,payload,priority,state"));
        assertTrue(csv.contains("\"'=SUM(1,2)\""));
        assertTrue(csv.contains("\"a\"\"b\nc\""));
        assertEquals(2, q.list().size());
    }

    @Test
    void jsonEscapesSpecialCharactersAndNulls() {
        ReliableTaskQueue q = queue(2);
        q.submit(ReliableTaskQueue.Request.immediate("escape", "hello\n\"world\"\\test"));
        String json = TaskQueueReports.jobsJson(q);
        assertTrue(json.startsWith("[{"));
        assertTrue(json.contains("hello\\n\\\"world\\\"\\\\test"));
        assertTrue(json.contains("\"expiresAt\":null"));
        assertTrue(json.contains("\"state\":\"READY\""));
        assertTrue(json.endsWith("}]"));
    }

    @Test
    void reportsSupportEmptyInputs() {
        assertEquals("[]", TaskQueueReports.jobsJson(List.of()));
        assertEquals("[]", TaskQueueReports.auditJson(List.of()));
        assertEquals("sequence,id,action,at,detail\n",
                TaskQueueReports.auditCsv(List.of()));
        assertTrue(TaskQueueReports.jobsCsv(List.of()).contains("last_error"));
    }

    @Test
    void auditReportsIncludeStateTransitions() {
        ReliableTaskQueue q = queue(1);
        q.submit(ReliableTaskQueue.Request.immediate("audit", "payload"));
        var lease = q.claim().orElseThrow();
        q.fail("audit", lease.token(), "problem");
        String csv = TaskQueueReports.auditCsv(q);
        String json = TaskQueueReports.auditJson(q.history());
        assertTrue(csv.contains("SUBMITTED"));
        assertTrue(csv.contains("CLAIMED"));
        assertTrue(csv.contains("DEAD_LETTER"));
        assertTrue(json.contains("\"action\":\"DEAD_LETTER\""));
        assertTrue(json.contains("\"detail\":\"problem\""));
    }

    @Test
    void prometheusAndStatsJsonExposeConsistentCounters() {
        ReliableTaskQueue q = queue(2);
        q.submit(ReliableTaskQueue.Request.immediate("metrics", "payload"));
        TaskQueueWorker.processOne(q, payload -> {});
        String metrics = TaskQueueReports.prometheus(q.stats());
        String json = TaskQueueReports.statsJson(q.stats());
        assertTrue(metrics.contains("task_queue_succeeded 1\n"));
        assertTrue(metrics.contains("task_queue_submissions_total 1\n"));
        assertTrue(metrics.contains("# TYPE task_queue_deliveries_total counter"));
        assertTrue(json.contains("\"succeeded\":1"));
        assertTrue(json.contains("\"ready\":0"));
        assertTrue(json.contains("\"deliveries\":1"));
    }

    @Test
    void reportingRejectsNullSources() {
        assertThrows(NullPointerException.class,
                () -> TaskQueueReports.jobsCsv((ReliableTaskQueue) null));
        assertThrows(NullPointerException.class,
                () -> TaskQueueReports.jobsJson((List<ReliableTaskQueue.View>) null));
        assertThrows(NullPointerException.class,
                () -> TaskQueueReports.auditCsv((List<ReliableTaskQueue.Event>) null));
        assertThrows(NullPointerException.class,
                () -> TaskQueueReports.statsJson(null));
        assertThrows(NullPointerException.class,
                () -> TaskQueueWorker.processOne(null, payload -> {}));
    }
}
