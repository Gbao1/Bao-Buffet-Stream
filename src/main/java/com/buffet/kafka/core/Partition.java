package com.buffet.kafka.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class Partition<T> {
    private final List<Message<T>> log = new ArrayList<>();
    private final AtomicLong nextOffset = new AtomicLong(0);

    public synchronized long append(String key, T payload) {
        long offset = nextOffset.getAndIncrement();
        log.add(new Message<>(offset, key, payload));
        return offset;
    }

    public synchronized List<Message<T>> readFrom(long offset, int maxBatch) {
        if (offset >= log.size()) {
            return List.of();
        }

        int start = (int) offset;
        int end = Math.min(log.size(), start + maxBatch);
        return new ArrayList<>(log.subList(start, end));
    }
}
