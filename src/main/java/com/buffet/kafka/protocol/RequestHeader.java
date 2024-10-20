package com.buffet.kafka.protocol;

import java.nio.ByteBuffer;

public final class RequestHeader {
    private final short apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final int clientId;

    public RequestHeader(short apiKey, short apiVersion, int correlationId, int clientId) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    public short getApiKey() {
        return apiKey;
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public int getClientId() {
        return clientId;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(apiKey);
        buffer.putShort(apiVersion);
        buffer.putInt(correlationId);
        buffer.putInt(clientId);
    }

    public static RequestHeader readFrom(ByteBuffer buffer) {
        short key = buffer.getShort();
        short version = buffer.getShort();
        int correlation = buffer.getInt();
        int client = buffer.getInt();
        return new RequestHeader(key, version, correlation, client);
    }
}
