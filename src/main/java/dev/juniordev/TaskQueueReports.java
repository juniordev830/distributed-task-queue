package dev.juniordev;

import java.util.List;
import java.util.Objects;

/**
 * Deterministic, dependency-free reporting for local diagnostics.
 * All output is generated from immutable queue snapshots.
 */
public final class TaskQueueReports {
    private TaskQueueReports() {}

    public static String jobsCsv(ReliableTaskQueue queue) {
        Objects.requireNonNull(queue, "queue");
        return jobsCsv(queue.list());
    }

    public static String jobsCsv(List<ReliableTaskQueue.View> jobs) {
        Objects.requireNonNull(jobs, "jobs");
        StringBuilder out = new StringBuilder(
                "id,payload,priority,state,attempts,available_at,expires_at,lease_until,last_error\n");
        for (ReliableTaskQueue.View job : jobs) {
            out.append(csv(job.id())).append(',')
                    .append(csv(job.payload())).append(',')
                    .append(job.priority()).append(',')
                    .append(job.state()).append(',')
                    .append(job.attempts()).append(',')
                    .append(job.availableAtMillis()).append(',')
                    .append(csv(job.expiresAtMillis())).append(',')
                    .append(csv(job.leaseUntilMillis())).append(',')
                    .append(csv(job.lastError())).append('\n');
        }
        return out.toString();
    }

    public static String auditCsv(ReliableTaskQueue queue) {
        Objects.requireNonNull(queue, "queue");
        return auditCsv(queue.history());
    }

    public static String auditCsv(List<ReliableTaskQueue.Event> events) {
        Objects.requireNonNull(events, "events");
        StringBuilder out = new StringBuilder("sequence,id,action,at,detail\n");
        for (ReliableTaskQueue.Event event : events) {
            out.append(event.sequence()).append(',')
                    .append(csv(event.id())).append(',')
                    .append(csv(event.action())).append(',')
                    .append(event.atMillis()).append(',')
                    .append(csv(event.detail())).append('\n');
        }
        return out.toString();
    }

    public static String jobsJson(ReliableTaskQueue queue) {
        Objects.requireNonNull(queue, "queue");
        return jobsJson(queue.list());
    }

    public static String jobsJson(List<ReliableTaskQueue.View> jobs) {
        Objects.requireNonNull(jobs, "jobs");
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < jobs.size(); i++) {
            ReliableTaskQueue.View job = jobs.get(i);
            if (i > 0) {
                out.append(',');
            }
            out.append('{');
            field(out, "id", job.id());
            out.append(',');
            field(out, "payload", job.payload());
            out.append(',');
            number(out, "priority", job.priority());
            out.append(',');
            field(out, "state", job.state().name());
            out.append(',');
            number(out, "attempts", job.attempts());
            out.append(',');
            number(out, "availableAt", job.availableAtMillis());
            out.append(',');
            nullableNumber(out, "expiresAt", job.expiresAtMillis());
            out.append(',');
            nullableNumber(out, "leaseUntil", job.leaseUntilMillis());
            out.append(',');
            field(out, "lastError", job.lastError());
            out.append('}');
        }
        return out.append(']').toString();
    }

    public static String auditJson(List<ReliableTaskQueue.Event> events) {
        Objects.requireNonNull(events, "events");
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < events.size(); i++) {
            ReliableTaskQueue.Event event = events.get(i);
            if (i > 0) {
                out.append(',');
            }
            out.append('{');
            number(out, "sequence", event.sequence());
            out.append(',');
            field(out, "id", event.id());
            out.append(',');
            field(out, "action", event.action());
            out.append(',');
            number(out, "at", event.atMillis());
            out.append(',');
            field(out, "detail", event.detail());
            out.append('}');
        }
        return out.append(']').toString();
    }

    public static String statsJson(ReliableTaskQueue.Stats stats) {
        Objects.requireNonNull(stats, "stats");
        StringBuilder out = new StringBuilder("{");
        number(out, "ready", stats.ready());
        out.append(',');
        number(out, "leased", stats.leased());
        out.append(',');
        number(out, "succeeded", stats.succeeded());
        out.append(',');
        number(out, "dead", stats.dead());
        out.append(',');
        number(out, "cancelled", stats.cancelled());
        out.append(',');
        number(out, "expired", stats.expired());
        out.append(',');
        number(out, "submissions", stats.submissions());
        out.append(',');
        number(out, "deliveries", stats.deliveries());
        out.append(',');
        number(out, "retries", stats.retries());
        out.append(',');
        number(out, "leaseTimeouts", stats.leaseTimeouts());
        return out.append('}').toString();
    }

    public static String prometheus(ReliableTaskQueue.Stats stats) {
        Objects.requireNonNull(stats, "stats");
        StringBuilder out = new StringBuilder();
        gauge(out, "task_queue_ready", stats.ready());
        gauge(out, "task_queue_leased", stats.leased());
        gauge(out, "task_queue_succeeded", stats.succeeded());
        gauge(out, "task_queue_dead", stats.dead());
        gauge(out, "task_queue_cancelled", stats.cancelled());
        gauge(out, "task_queue_expired", stats.expired());
        counter(out, "task_queue_submissions_total", stats.submissions());
        counter(out, "task_queue_deliveries_total", stats.deliveries());
        counter(out, "task_queue_retries_total", stats.retries());
        counter(out, "task_queue_lease_timeouts_total", stats.leaseTimeouts());
        return out.toString();
    }

    private static void gauge(StringBuilder out, String name, long value) {
        out.append("# TYPE ").append(name).append(" gauge\n");
        out.append(name).append(' ').append(value).append('\n');
    }

    private static void counter(StringBuilder out, String name, long value) {
        out.append("# TYPE ").append(name).append(" counter\n");
        out.append(name).append(' ').append(value).append('\n');
    }

    private static void number(StringBuilder out, String key, long value) {
        out.append('"').append(key).append("\":").append(value);
    }

    private static void nullableNumber(StringBuilder out, String key, Long value) {
        out.append('"').append(key).append("\":");
        out.append(value == null ? "null" : value.toString());
    }

    private static void field(StringBuilder out, String key, String value) {
        out.append('"').append(key).append("\":");
        out.append(value == null ? "null" : json(value));
    }

    private static String csv(Object value) {
        if (value == null) {
            return "";
        }
        String text = value.toString();
        if (!text.isEmpty() && "=+-@\t\r\n".indexOf(text.charAt(0)) >= 0) {
            text = "'" + text;
        }
        if (text.contains(",") || text.contains("\"") || text.contains("\n")
                || text.contains("\r")) {
            return "\"" + text.replace("\"", "\"\"") + "\"";
        }
        return text;
    }

    private static String json(String text) {
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }
}
