package com.buffet.kafka.protocol;

import java.nio.ByteBuffer;

public final class ResponseApiBody {
    private final byte apiKeysLength;
    private final short apiKey;
    private final short minVersion;
    private final short maxVersion;
    private final byte apiKeyTags;

    public ResponseApiBody(short apiKey, short minVersion, short maxVersion) {
        this.apiKeysLength = 0x02;
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
        this.apiKeyTags = 0x00;
    }

    public short getApiKey() {
        return apiKey;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.put(apiKeysLength);
        buffer.putShort(apiKey);
        buffer.putShort(minVersion);
        buffer.putShort(maxVersion);
        buffer.put(apiKeyTags);
    }
}
