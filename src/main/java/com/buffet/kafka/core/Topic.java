package com.buffet.kafka.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public final class Topic<T> {
    private final String name;
    private final List<Partition<T>> partitions;

    public Topic(String name, int partitionCount) {
        this.name = name;
        this.partitions = new ArrayList<>(partitionCount);

        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new Partition<>());
        }
    }

    public String getName() {
        return name;
    }

    public List<Partition<T>> getPartitions() {
        return partitions;
    }

    public int partitionForKey(String key) {
        if (key == null) {
            return ThreadLocalRandom.current().nextInt(partitions.size());
        }

        return Math.floorMod(key.hashCode(), partitions.size());
    }
}
