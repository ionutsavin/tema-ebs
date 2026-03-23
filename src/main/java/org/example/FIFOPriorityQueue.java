package org.example;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class FIFOPriorityQueue<T> {

    private static class Entry<T> implements Comparable<Entry<T>> {
        final int priority;
        final long count;
        final T item;

        Entry(int priority, long count, T item) {
            this.priority = priority;
            this.count = count;
            this.item = item;
        }

        @Override
        public int compareTo(Entry<T> other) {
            int cmp = Integer.compare(this.priority, other.priority);
            if (cmp != 0) return cmp;
            return Long.compare(this.count, other.count);
        }
    }

    private final PriorityBlockingQueue<Entry<T>> queue;
    private final AtomicLong counter;

    public FIFOPriorityQueue() {
        this.queue = new PriorityBlockingQueue<>();
        this.counter = new AtomicLong(0);
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    public void push(T item, int priority) {
        long count = counter.getAndIncrement();
        queue.put(new Entry<>(priority, count, item));
    }

    public T pop() {
        Entry<T> entry = queue.poll();
        if (entry == null) {
            throw new IndexOutOfBoundsException("pop from an empty FIFO priority queue");
        }
        return entry.item;
    }

    public T peek() {
        Entry<T> entry = queue.peek();
        if (entry == null) {
            throw new IndexOutOfBoundsException("peek from an empty FIFO priority queue");
        }
        return entry.item;
    }
}
