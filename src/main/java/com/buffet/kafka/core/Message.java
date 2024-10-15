package com.buffet.kafka.core;

import java.time.Instant;

public final class Message<T> {
    private final long offset;
    private final String key;
    private final T payload;
    private final Instant timestamp;

    public Message(long offset, String key, T payload) {
        this.offset = offset;
        this.key = key;
        this.payload = payload;
        this.timestamp = Instant.now();
    }

    public long getOffset() {
        return offset;
    }

    public String getKey() {
        return key;
    }

    public T getPayload() {
        return payload;
    }

    public Instant getTimestamp() {
        return timestamp;
    }
}
