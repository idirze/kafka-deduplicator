package com.idirze.kafka.deduplicator.extractor;

public class FunctionalKeyExtractor<K, V> {

    /**
     * Build the logic to extract and build the functional key
     *
     * @param key
     * @param value
     * @return
     */
    public V extract(K key, V value) {
        return value;
    }
}
