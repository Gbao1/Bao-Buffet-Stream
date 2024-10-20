package com.buffet.kafka.protocol;

import java.nio.ByteBuffer;

public final class ResponseHeader {
    private final int correlationId;

    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
    }
}
