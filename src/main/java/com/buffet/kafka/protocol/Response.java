package com.buffet.kafka.protocol;

import java.nio.ByteBuffer;

public final class Response {
    private final int messageSize;
    private final ResponseHeader header;
    private final ResponseBody body;

    public Response(ResponseHeader header, ResponseBody body) {
        this.header = header;
        this.body = body;
        this.messageSize = 4 + body.byteSize();
    }

    public ResponseHeader getHeader() {
        return header;
    }

    public ResponseBody getBody() {
        return body;
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteCodec.allocate(4 + messageSize);
        buffer.putInt(messageSize);
        header.writeTo(buffer);
        body.writeTo(buffer);
        return ByteCodec.toArray(buffer);
    }

    @Override
    public String toString() {
        return "Response{" +
                "messageSize=" + messageSize +
                ", correlationId=" + header.getCorrelationId() +
                ", errorCode=" + body.getErrorCode() +
                ", apiCount=" + body.getApiBody().size() +
                '}';
    }
}
