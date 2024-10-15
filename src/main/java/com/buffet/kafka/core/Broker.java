package com.buffet.kafka.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Broker {
    private final Map<String, Topic<?>> topics = new ConcurrentHashMap<>();

    public <T> void createTopic(String name, int partitions) {
        topics.putIfAbsent(name, new Topic<T>(name, partitions));
    }

    @SuppressWarnings("unchecked")
    public <T> Topic<T> topic(String name) {
        Topic<T> topic = (Topic<T>) topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + name);
        }
        return topic;
    }
}
