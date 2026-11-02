package dev.juniordev;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Objects;
import java.util.function.Predicate;

/** Single-process job queue demo. No persistence or RabbitMQ integration yet. */
public final class TaskQueue {
    public record Job(String id, String payload, int attempts) {}
    public enum Result { SUCCESS, RETRYING, DEAD_LETTER, EMPTY }
    private final Queue<Job> ready = new ArrayDeque<>();
    private final Queue<Job> dead = new ArrayDeque<>();
    private final int maxAttempts;
    public TaskQueue(int maxAttempts) {
        if (maxAttempts < 1 || maxAttempts > 100) throw new IllegalArgumentException("maxAttempts 1..100");
        this.maxAttempts = maxAttempts;
    }
    public synchronized void submit(String id, String payload) {
        if (id == null || id.isBlank() || payload == null || payload.isBlank())
            throw new IllegalArgumentException("id and payload required");
        ready.add(new Job(id.trim(), payload, 0));
    }
    public synchronized Result processNext(Predicate<String> worker) {
        Objects.requireNonNull(worker);
        Job job = ready.poll();
        if (job == null) return Result.EMPTY;
        boolean succeeded;
        try { succeeded = worker.test(job.payload()); }
        catch (RuntimeException error) { succeeded = false; }
        if (succeeded) return Result.SUCCESS;
        Job attempted = new Job(job.id(), job.payload(), job.attempts() + 1);
        if (attempted.attempts() >= maxAttempts) {
            dead.add(attempted); return Result.DEAD_LETTER;
        }
        ready.add(attempted); return Result.RETRYING;
    }
    public synchronized int pending() { return ready.size(); }
    public synchronized int deadLetters() { return dead.size(); }
}
