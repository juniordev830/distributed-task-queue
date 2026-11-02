package dev.juniordev;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class TaskQueueTest {
    @Test void retriesAndMovesToDeadLetter() {
        TaskQueue q = new TaskQueue(2);
        q.submit("id-1", "payload");
        assertEquals(TaskQueue.Result.RETRYING, q.processNext(s -> false));
        assertEquals(TaskQueue.Result.DEAD_LETTER, q.processNext(s -> false));
        assertEquals(1, q.deadLetters());
        assertEquals(TaskQueue.Result.EMPTY, q.processNext(s -> true));
    }
    @Test void successfulJobIsRemoved() {
        TaskQueue q = new TaskQueue(3);
        q.submit("id-2", "ok");
        assertEquals(TaskQueue.Result.SUCCESS, q.processNext(s -> s.equals("ok")));
        assertEquals(0, q.pending());
    }
    @Test void validatesInput() {
        TaskQueue q = new TaskQueue(2);
        assertThrows(IllegalArgumentException.class, () -> q.submit("", "x"));
        assertThrows(IllegalArgumentException.class, () -> new TaskQueue(0));
    }
}
