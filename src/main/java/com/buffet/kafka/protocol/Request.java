package com.buffet.kafka.protocol;

import java.nio.ByteBuffer;

public final class Request {
    private static final int HEADER_SIZE_BYTES = 12;

    private final int messageSize;
    private final RequestHeader header;

    public Request(RequestHeader header) {
        this(HEADER_SIZE_BYTES, header);
    }

    public Request(int messageSize, RequestHeader header) {
        this.messageSize = messageSize;
        this.header = header;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public RequestHeader getHeader() {
        return header;
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteCodec.allocate(4 + HEADER_SIZE_BYTES);
        buffer.putInt(messageSize);
        header.writeTo(buffer);
        return ByteCodec.toArray(buffer);
    }

    public static Request deserialize(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int size = buffer.getInt();
        RequestHeader header = RequestHeader.readFrom(buffer);
        return new Request(size, header);
    }

    @Override
    public String toString() {
        return "Request{" +
                "messageSize=" + messageSize +
                ", apiKey=" + header.getApiKey() +
                ", apiVersion=" + header.getApiVersion() +
                ", correlationId=" + header.getCorrelationId() +
                ", clientId=" + header.getClientId() +
                '}';
    }
}
