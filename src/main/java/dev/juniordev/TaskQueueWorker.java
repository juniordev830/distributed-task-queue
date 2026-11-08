package dev.juniordev;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Runs caller-provided handlers without holding any queue lock.
 * The queue owns delivery state; the runner owns handler invocation.
 */
public final class TaskQueueWorker {
    private TaskQueueWorker() {}

    @FunctionalInterface
    public interface Handler {
        void handle(String payload) throws Exception;
    }

    public enum Outcome {
        NO_JOB, SUCCEEDED, RETRY_SCHEDULED, DEAD_LETTER, EXPIRED, STALE
    }

    public record Attempt(String id, int attempt, Outcome outcome, String error) {
        public boolean succeeded() {
            return outcome == Outcome.SUCCEEDED;
        }
        public boolean failed() {
            return outcome == Outcome.RETRY_SCHEDULED
                    || outcome == Outcome.DEAD_LETTER || outcome == Outcome.EXPIRED;
        }
    }

    public record Batch(List<Attempt> attempts) {
        public Batch {
            attempts = List.copyOf(attempts);
        }
        public long successes() {
            return attempts.stream().filter(Attempt::succeeded).count();
        }
        public long failures() {
            return attempts.stream().filter(Attempt::failed).count();
        }
        public long stale() {
            return attempts.stream().filter(a -> a.outcome() == Outcome.STALE).count();
        }
        public int size() {
            return attempts.size();
        }
    }

    public static Attempt processOne(ReliableTaskQueue queue, Handler handler) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(handler, "handler");
        return queue.claim()
                .map(lease -> execute(queue, handler, lease))
                .orElse(new Attempt("", 0, Outcome.NO_JOB, ""));
    }

    public static Attempt execute(ReliableTaskQueue queue, Handler handler,
                                  ReliableTaskQueue.Lease lease) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(lease, "lease");
        try {
            handler.handle(lease.payload());
            boolean acknowledged = queue.acknowledge(lease.id(), lease.token());
            return new Attempt(lease.id(), lease.attempt(),
                    acknowledged ? Outcome.SUCCEEDED : Outcome.STALE, "");
        } catch (Exception error) {
            String message = error.getClass().getSimpleName()
                    + (error.getMessage() == null ? "" : ": " + error.getMessage());
            ReliableTaskQueue.FailureOutcome result =
                    queue.fail(lease.id(), lease.token(), message);
            return new Attempt(lease.id(), lease.attempt(), map(result), message);
        }
    }

    private static Outcome map(ReliableTaskQueue.FailureOutcome outcome) {
        return switch (outcome) {
            case RETRY_SCHEDULED -> Outcome.RETRY_SCHEDULED;
            case DEAD_LETTER -> Outcome.DEAD_LETTER;
            case EXPIRED -> Outcome.EXPIRED;
            case STALE -> Outcome.STALE;
        };
    }

    public static Batch drain(ReliableTaskQueue queue, Handler handler, int maxJobs) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(handler, "handler");
        if (maxJobs < 1 || maxJobs > 10_000) {
            throw new IllegalArgumentException("maxJobs must be 1..10000");
        }
        List<Attempt> attempts = new ArrayList<>();
        for (int i = 0; i < maxJobs; i++) {
            Attempt attempt = processOne(queue, handler);
            if (attempt.outcome() == Outcome.NO_JOB) {
                break;
            }
            attempts.add(attempt);
        }
        return new Batch(attempts);
    }

    /**
     * Claims at most parallelism jobs before submitting workers.
     * The supplied executor's lifetime belongs to the caller.
     */
    public static Batch parallel(ReliableTaskQueue queue, Handler handler,
                                 ExecutorService executor, int parallelism)
            throws InterruptedException {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(executor, "executor");
        if (parallelism < 1 || parallelism > 256) {
            throw new IllegalArgumentException("parallelism must be 1..256");
        }
        List<ReliableTaskQueue.Lease> leases = queue.claimBatch(parallelism);
        List<Future<Attempt>> futures = new ArrayList<>();
        for (ReliableTaskQueue.Lease lease : leases) {
            futures.add(executor.submit(() -> execute(queue, handler, lease)));
        }
        List<Attempt> attempts = new ArrayList<>();
        for (Future<Attempt> future : futures) {
            try {
                attempts.add(future.get());
            } catch (ExecutionException failure) {
                throw new IllegalStateException("unexpected worker failure", failure.getCause());
            }
        }
        return new Batch(attempts);
    }
}
