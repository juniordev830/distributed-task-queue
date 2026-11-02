package dev.juniordev;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TaskQueueTest {
    @Test
    void retriesAndMovesToDeadLetter() {
        TaskQueue q = new TaskQueue(2);
        q.submit("id-1", "payload");
        assertEquals(TaskQueue.Result.RETRYING, q.processNext(s -> false));
        assertEquals(TaskQueue.Result.DEAD_LETTER, q.processNext(s -> false));
        assertEquals(1, q.deadLetters());
        assertEquals(TaskQueue.Result.EMPTY, q.processNext(s -> true));
    }

    @Test
    void successfulJobIsRemoved() {
        TaskQueue q = new TaskQueue(3);
        q.submit("id-2", "ok");
        assertEquals(TaskQueue.Result.SUCCESS, q.processNext(s -> s.equals("ok")));
        assertEquals(0, q.pending());
    }

    @Test
    void validatesInput() {
        TaskQueue q = new TaskQueue(2);
        assertThrows(IllegalArgumentException.class, () -> q.submit("", "x"));
        assertThrows(IllegalArgumentException.class, () -> new TaskQueue(0));
        assertThrows(NullPointerException.class, () -> q.processNext(null));
    }

    @Test
    void failedWorkerExceptionsAreRetried() {
        TaskQueue q = new TaskQueue(2);
        q.submit("broken", "payload");
        assertEquals(TaskQueue.Result.RETRYING, q.processNext(s -> {
            throw new IllegalStateException("temporary failure");
        }));
        assertEquals(1, q.pending());
        assertEquals(TaskQueue.Result.DEAD_LETTER, q.processNext(s -> false));
        assertEquals(1, q.deadLetters());
    }

    @Test
    void slowWorkerDoesNotBlockOtherQueueOperations() throws Exception {
        TaskQueue q = new TaskQueue(2);
        q.submit("slow", "first");
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<TaskQueue.Result> processing = executor.submit(() ->
                q.processNext(payload -> {
                    workerStarted.countDown();
                    try {
                        return releaseWorker.await(15, TimeUnit.SECONDS);
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                })
            );
            assertTrue(workerStarted.await(5, TimeUnit.SECONDS), "worker should start");

            Future<Integer> submission = executor.submit(() -> {
                q.submit("new", "second");
                return q.pending();
            });
            assertEquals(1, submission.get(5, TimeUnit.SECONDS),
                    "slow processing must not block a concurrent submission");
            releaseWorker.countDown();
            assertEquals(TaskQueue.Result.SUCCESS, processing.get(5, TimeUnit.SECONDS));
            assertEquals(1, q.pending());
        } finally {
            releaseWorker.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
