package com.buffet.kafka.core;

public final class Producer {
    private final Broker broker;

    public Producer(Broker broker) {
        this.broker = broker;
    }

    public <T> void send(String topicName, String key, T payload) {
        Topic<T> topic = broker.topic(topicName);
        int partition = topic.partitionForKey(key);
        long offset = topic.getPartitions().get(partition).append(key, payload);

        System.out.printf("[PRODUCE] topic=%s partition=%d offset=%d key=%s payload=%s%n",
                topicName, partition, offset, key, payload);
    }
}
