package com.buffet.kafka.protocol;

public enum ApiKey {
    API_VERSIONS(18),
    DESCRIBE_TOPIC_PARTITIONS(75);

    private final int id;

    ApiKey(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static ApiKey fromId(int id) {
        for (ApiKey key : values()) {
            if (key.id == id) {
                return key;
            }
        }
        throw new IllegalArgumentException("Unsupported api key: " + id);
    }
}
