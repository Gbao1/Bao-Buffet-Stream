package com.buffet.kafka.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class ByteCodec {
    private ByteCodec() {
    }

    public static ByteBuffer allocate(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN);
    }

    public static byte[] toArray(ByteBuffer buffer) {
        byte[] payload = new byte[buffer.position()];
        buffer.flip();
        buffer.get(payload);
        return payload;
    }
}
