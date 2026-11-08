package dev.juniordev;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * An in-process, bounded, lease-based queue with deterministic clock injection.
 * This is deliberately not a distributed broker: all state is lost on restart.
 * No user callback runs under the queue monitor.
 */
public final class ReliableTaskQueue {
    public enum State { READY, LEASED, SUCCEEDED, DEAD, CANCELLED, EXPIRED }
    public enum FailureOutcome { RETRY_SCHEDULED, DEAD_LETTER, EXPIRED, STALE }

    public record Config(
            int capacity,
            int maxAttempts,
            Duration visibilityTimeout,
            Duration initialBackoff,
            Duration maxBackoff,
            int maxPayloadChars,
            int historyLimit) {
        public Config {
            if (capacity < 1 || capacity > 100_000) {
                throw new IllegalArgumentException("capacity must be 1..100000");
            }
            if (maxAttempts < 1 || maxAttempts > 100) {
                throw new IllegalArgumentException("maxAttempts must be 1..100");
            }
            Objects.requireNonNull(visibilityTimeout, "visibilityTimeout");
            Objects.requireNonNull(initialBackoff, "initialBackoff");
            Objects.requireNonNull(maxBackoff, "maxBackoff");
            if (visibilityTimeout.isZero() || visibilityTimeout.isNegative()
                    || visibilityTimeout.toMillis() < 1) {
                throw new IllegalArgumentException("visibilityTimeout must be positive");
            }
            if (initialBackoff.isNegative() || maxBackoff.isNegative()
                    || initialBackoff.compareTo(maxBackoff) > 0) {
                throw new IllegalArgumentException("invalid retry backoff");
            }
            if (maxPayloadChars < 1 || maxPayloadChars > 1_000_000) {
                throw new IllegalArgumentException("invalid payload limit");
            }
            if (historyLimit < 1 || historyLimit > 100_000) {
                throw new IllegalArgumentException("invalid history limit");
            }
            visibilityTimeout.toMillis();
            initialBackoff.toMillis();
            maxBackoff.toMillis();
        }

        public static Config defaults() {
            return new Config(1_000, 3, Duration.ofSeconds(30),
                    Duration.ofSeconds(1), Duration.ofMinutes(5), 16_384, 2_000);
        }
    }

    public record Request(String id, String payload, int priority,
                          Duration delay, Duration ttl) {
        public Request {
            Objects.requireNonNull(delay, "delay");
            if (delay.isNegative()) {
                throw new IllegalArgumentException("delay must not be negative");
            }
            if (ttl != null && (ttl.isZero() || ttl.isNegative())) {
                throw new IllegalArgumentException("ttl must be positive");
            }
        }

        public static Request immediate(String id, String payload) {
            return new Request(id, payload, 0, Duration.ZERO, null);
        }
    }

    public record Lease(String id, String payload, String token,
                        int attempt, long deadlineMillis) {}
    public record View(String id, String payload, int priority, State state,
                       int attempts, long availableAtMillis, Long expiresAtMillis,
                       Long leaseUntilMillis, String lastError) {}
    public record Event(long sequence, String id, String action,
                        long atMillis, String detail) {}
    public record Stats(int ready, int leased, int succeeded, int dead,
                        int cancelled, int expired, long submissions,
                        long deliveries, long retries, long leaseTimeouts) {
        public int active() {
            return ready + leased;
        }
        public int terminal() {
            return succeeded + dead + cancelled + expired;
        }
    }

    private static final class Entry {
        private final String id;
        private final String payload;
        private final int priority;
        private final long sequence;
        private final Long expiresAt;
        private State state = State.READY;
        private long availableAt;
        private Long leaseUntil;
        private String token;
        private String lastError;
        private int attempts;
        private long changedAt;

        private Entry(Request request, long sequence, long now, long availableAt,
                      Long expiresAt) {
            id = request.id().trim();
            payload = request.payload();
            priority = request.priority();
            this.sequence = sequence;
            this.availableAt = availableAt;
            this.expiresAt = expiresAt;
            changedAt = now;
        }

        private View view() {
            return new View(id, payload, priority, state, attempts, availableAt,
                    expiresAt, leaseUntil, lastError);
        }
    }

    private final Config config;
    private final Clock clock;
    private final Map<String, Entry> entries = new LinkedHashMap<>();
    private final ArrayDeque<Event> events = new ArrayDeque<>();
    private long sequence;
    private long eventSequence;
    private long submissions;
    private long deliveries;
    private long retries;
    private long leaseTimeouts;

    public ReliableTaskQueue(Config config, Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public ReliableTaskQueue() {
        this(Config.defaults(), Clock.systemUTC());
    }

    public Config config() {
        return config;
    }

    public synchronized boolean submit(Request request) {
        Objects.requireNonNull(request, "request");
        validate(request);
        sweepInternal(clock.millis());
        String id = request.id().trim();
        if (entries.containsKey(id)) {
            return false;
        }
        if (activeCount() >= config.capacity()) {
            throw new IllegalStateException("queue capacity reached");
        }
        long now = clock.millis();
        long availableAt = plus(now, request.delay().toMillis());
        Long expiresAt = request.ttl() == null ? null : plus(now, request.ttl().toMillis());
        if (expiresAt != null && availableAt >= expiresAt) {
            throw new IllegalArgumentException("delay must be shorter than ttl");
        }
        Entry entry = new Entry(request, ++sequence, now, availableAt, expiresAt);
        entries.put(id, entry);
        submissions++;
        event(entry, "SUBMITTED", "priority=" + request.priority());
        return true;
    }

    private void validate(Request request) {
        if (request.id() == null || !request.id().matches("[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}")) {
            throw new IllegalArgumentException("invalid job id");
        }
        if (request.payload() == null || request.payload().isBlank()
                || request.payload().length() > config.maxPayloadChars()) {
            throw new IllegalArgumentException("invalid payload");
        }
        if (request.priority() < -100 || request.priority() > 100) {
            throw new IllegalArgumentException("priority must be -100..100");
        }
        request.delay().toMillis();
        if (request.ttl() != null) {
            request.ttl().toMillis();
        }
    }

    private int activeCount() {
        int count = 0;
        for (Entry entry : entries.values()) {
            if (entry.state == State.READY || entry.state == State.LEASED) {
                count++;
            }
        }
        return count;
    }

    public synchronized Optional<Lease> claim() {
        long now = clock.millis();
        sweepInternal(now);
        Entry candidate = entries.values().stream()
                .filter(e -> e.state == State.READY && e.availableAt <= now)
                .min(Comparator.comparingInt((Entry e) -> e.priority).reversed()
                        .thenComparingLong(e -> e.availableAt)
                        .thenComparingLong(e -> e.sequence))
                .orElse(null);
        if (candidate == null) {
            return Optional.empty();
        }
        candidate.state = State.LEASED;
        candidate.attempts++;
        candidate.token = UUID.randomUUID().toString();
        candidate.leaseUntil = plus(now, config.visibilityTimeout().toMillis());
        candidate.changedAt = now;
        deliveries++;
        event(candidate, "CLAIMED", "attempt=" + candidate.attempts);
        return Optional.of(new Lease(candidate.id, candidate.payload, candidate.token,
                candidate.attempts, candidate.leaseUntil));
    }

    public synchronized List<Lease> claimBatch(int limit) {
        if (limit < 1 || limit > 1000) {
            throw new IllegalArgumentException("batch size must be 1..1000");
        }
        List<Lease> leases = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            Optional<Lease> lease = claim();
            if (lease.isEmpty()) {
                break;
            }
            leases.add(lease.orElseThrow());
        }
        return List.copyOf(leases);
    }

    public synchronized boolean acknowledge(String id, String token) {
        sweepInternal(clock.millis());
        Entry entry = currentLease(id, token);
        if (entry == null) {
            return false;
        }
        terminal(entry, State.SUCCEEDED, "ACKNOWLEDGED");
        return true;
    }

    public synchronized FailureOutcome fail(String id, String token, String reason) {
        long now = clock.millis();
        sweepInternal(now);
        Entry entry = currentLease(id, token);
        if (entry == null) {
            return FailureOutcome.STALE;
        }
        entry.lastError = sanitize(reason);
        if (entry.attempts >= config.maxAttempts()) {
            terminal(entry, State.DEAD, "DEAD_LETTER");
            return FailureOutcome.DEAD_LETTER;
        }
        long retryAt = plus(now, backoffMillis(entry.attempts));
        if (entry.expiresAt != null && retryAt >= entry.expiresAt) {
            terminal(entry, State.EXPIRED, "EXPIRED_BEFORE_RETRY");
            return FailureOutcome.EXPIRED;
        }
        entry.state = State.READY;
        entry.availableAt = retryAt;
        entry.leaseUntil = null;
        entry.token = null;
        entry.changedAt = now;
        retries++;
        event(entry, "RETRY_SCHEDULED", entry.lastError);
        return FailureOutcome.RETRY_SCHEDULED;
    }

    public synchronized boolean extendLease(String id, String token, Duration extension) {
        Objects.requireNonNull(extension, "extension");
        if (extension.isZero() || extension.isNegative() || extension.toMillis() < 1) {
            throw new IllegalArgumentException("extension must be positive");
        }
        long now = clock.millis();
        sweepInternal(now);
        Entry entry = currentLease(id, token);
        if (entry == null) {
            return false;
        }
        long until = plus(now, extension.toMillis());
        if (entry.expiresAt != null) {
            until = Math.min(until, entry.expiresAt);
        }
        if (until <= now) {
            return false;
        }
        entry.leaseUntil = Math.max(entry.leaseUntil, until);
        entry.changedAt = now;
        event(entry, "LEASE_EXTENDED", "until=" + entry.leaseUntil);
        return true;
    }

    public synchronized boolean cancel(String id) {
        sweepInternal(clock.millis());
        Entry entry = entries.get(id);
        if (entry == null || (entry.state != State.READY && entry.state != State.LEASED)) {
            return false;
        }
        terminal(entry, State.CANCELLED, "CANCELLED");
        return true;
    }

    public synchronized boolean replayDeadLetter(String id) {
        sweepInternal(clock.millis());
        Entry entry = entries.get(id);
        if (entry == null || entry.state != State.DEAD) {
            return false;
        }
        if (activeCount() >= config.capacity()) {
            throw new IllegalStateException("queue capacity reached");
        }
        if (entry.expiresAt != null && entry.expiresAt <= clock.millis()) {
            return false;
        }
        entry.state = State.READY;
        entry.attempts = 0;
        entry.availableAt = clock.millis();
        entry.lastError = null;
        entry.changedAt = clock.millis();
        event(entry, "REPLAYED", "operator replay");
        return true;
    }

    public synchronized int sweep() {
        return sweepInternal(clock.millis());
    }

    private int sweepInternal(long now) {
        int changed = 0;
        for (Entry entry : entries.values()) {
            if (entry.state != State.READY && entry.state != State.LEASED) {
                continue;
            }
            if (entry.expiresAt != null && now >= entry.expiresAt) {
                terminal(entry, State.EXPIRED, "TTL_EXPIRED");
                changed++;
                continue;
            }
            if (entry.state == State.LEASED && entry.leaseUntil <= now) {
                leaseTimeouts++;
                entry.lastError = "visibility timeout";
                if (entry.attempts >= config.maxAttempts()) {
                    terminal(entry, State.DEAD, "LEASE_TIMEOUT_DEAD");
                } else {
                    entry.state = State.READY;
                    entry.availableAt = plus(now, backoffMillis(entry.attempts));
                    entry.leaseUntil = null;
                    entry.token = null;
                    entry.changedAt = now;
                    retries++;
                    event(entry, "LEASE_TIMEOUT_RETRY", "attempt=" + entry.attempts);
                }
                changed++;
            }
        }
        return changed;
    }

    private Entry currentLease(String id, String token) {
        Entry entry = entries.get(id);
        if (entry == null || entry.state != State.LEASED || token == null
                || !token.equals(entry.token)) {
            return null;
        }
        return entry;
    }

    private void terminal(Entry entry, State state, String action) {
        entry.state = state;
        entry.leaseUntil = null;
        entry.token = null;
        entry.changedAt = clock.millis();
        event(entry, action, entry.lastError == null ? "" : entry.lastError);
    }

    private long backoffMillis(int attempts) {
        long delay = config.initialBackoff().toMillis();
        long maximum = config.maxBackoff().toMillis();
        for (int i = 1; i < attempts && delay < maximum; i++) {
            delay = delay > maximum / 2 ? maximum : delay * 2;
        }
        return Math.min(delay, maximum);
    }

    private static long plus(long now, long millis) {
        try {
            return Math.addExact(now, millis);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static String sanitize(String reason) {
        if (reason == null || reason.isBlank()) {
            return "worker reported failure";
        }
        String cleaned = reason.replaceAll("[\\r\\n\\t\\p{Cntrl}]", " ").trim();
        return cleaned.length() > 160 ? cleaned.substring(0, 160) : cleaned;
    }

    private void event(Entry entry, String action, String detail) {
        events.addLast(new Event(++eventSequence, entry.id, action, clock.millis(), detail));
        while (events.size() > config.historyLimit()) {
            events.removeFirst();
        }
    }

    public synchronized Optional<View> inspect(String id) {
        sweepInternal(clock.millis());
        Entry entry = entries.get(id);
        return entry == null ? Optional.empty() : Optional.of(entry.view());
    }

    public synchronized List<View> list() {
        sweepInternal(clock.millis());
        return entries.values().stream().map(Entry::view)
                .sorted(Comparator.comparing(View::id)).toList();
    }

    public synchronized List<View> list(State state) {
        Objects.requireNonNull(state, "state");
        return list().stream().filter(view -> view.state() == state).toList();
    }

    public synchronized List<Event> history() {
        return List.copyOf(events);
    }

    public synchronized List<Event> history(String id) {
        return events.stream().filter(event -> event.id().equals(id)).toList();
    }

    public synchronized Stats stats() {
        sweepInternal(clock.millis());
        int ready = 0;
        int leased = 0;
        int succeeded = 0;
        int dead = 0;
        int cancelled = 0;
        int expired = 0;
        for (Entry entry : entries.values()) {
            switch (entry.state) {
                case READY -> ready++;
                case LEASED -> leased++;
                case SUCCEEDED -> succeeded++;
                case DEAD -> dead++;
                case CANCELLED -> cancelled++;
                case EXPIRED -> expired++;
            }
        }
        return new Stats(ready, leased, succeeded, dead, cancelled, expired,
                submissions, deliveries, retries, leaseTimeouts);
    }

    public synchronized int purgeTerminal(Duration olderThan) {
        Objects.requireNonNull(olderThan, "olderThan");
        if (olderThan.isNegative()) {
            throw new IllegalArgumentException("olderThan must not be negative");
        }
        sweepInternal(clock.millis());
        long threshold = clock.millis() - olderThan.toMillis();
        int before = entries.size();
        entries.values().removeIf(entry ->
                entry.state != State.READY && entry.state != State.LEASED
                        && entry.changedAt <= threshold);
        return before - entries.size();
    }
}
