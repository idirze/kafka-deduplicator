package com.idirze.kafka.deduplicator.partitioner;

import org.apache.kafka.streams.processor.StreamPartitioner;

public class SourceTopicPartitionsRepartitioner implements StreamPartitioner<String, String> {

    public Integer partition(String key, String value, String s3, int numPartitions) {
        return key.hashCode() % numPartitions;
    }
}
