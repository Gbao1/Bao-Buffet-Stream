package com.buffet.kafka.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class ConsumerGroup<T> {
    private final String groupId;
    private final Broker broker;
    private final String topicName;
    private final Consumer<Message<T>> handler;
    private final int maxBatch;
    private final long pollMs;
    private final Map<Integer, Long> committedOffsets = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread worker;

    public ConsumerGroup(String groupId, Broker broker, String topicName,
                         Consumer<Message<T>> handler, int maxBatch, long pollMs) {
        this.groupId = groupId;
        this.broker = broker;
        this.topicName = topicName;
        this.handler = handler;
        this.maxBatch = maxBatch;
        this.pollMs = pollMs;
    }

    public void start() {
        if (running.getAndSet(true)) {
            return;
        }

        Topic<T> topic = broker.topic(topicName);
        for (int p = 0; p < topic.getPartitions().size(); p++) {
            committedOffsets.putIfAbsent(p, 0L);
        }

        worker = new Thread(this::pollLoop, "consumer-group-" + groupId);
        worker.setDaemon(true);
        worker.start();
    }

    public void stop() {
        running.set(false);
        if (worker != null) {
            worker.interrupt();
        }
    }

    private void pollLoop() {
        while (running.get()) {
            Topic<T> topic = broker.topic(topicName);
            for (int p = 0; p < topic.getPartitions().size(); p++) {
                long nextOffset = committedOffsets.getOrDefault(p, 0L);
                List<Message<T>> batch = topic.getPartitions().get(p).readFrom(nextOffset, maxBatch);

                for (Message<T> message : batch) {
                    handler.accept(message);
                    committedOffsets.put(p, message.getOffset() + 1);
                }
            }

            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
