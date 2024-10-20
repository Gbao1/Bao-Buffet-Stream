package com.buffet.kafka.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ResponseBody {
    private short errorCode;
    private final List<ResponseApiBody> apiBody;
    private int throttleTimeMs;
    private byte noTags;

    public ResponseBody() {
        this.errorCode = 0;
        this.apiBody = new ArrayList<>();
        this.throttleTimeMs = 0;
        this.noTags = 0x00;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void addApi(ResponseApiBody api) {
        apiBody.add(api);
    }

    public List<ResponseApiBody> getApiBody() {
        return Collections.unmodifiableList(apiBody);
    }

    public int byteSize() {
        return 2 + 1 + (apiBody.size() * 8) + 4 + 1;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(errorCode);
        buffer.put((byte) apiBody.size());
        for (ResponseApiBody api : apiBody) {
            api.writeTo(buffer);
        }
        buffer.putInt(throttleTimeMs);
        buffer.put(noTags);
    }
}
