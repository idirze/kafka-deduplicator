package com.idirze.kafka.deduplicator.processor;

import com.idirze.kafka.deduplicator.extractor.FunctionalKeyExtractor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;

public class DeduplicateProcessor extends AbstractProcessor<String, String> {

    private ProcessorContext context;
    private KeyValueStore<String, String> deduplicateStore;


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        deduplicateStore = (KeyValueStore) context.getStateStore("deduplicate-store");
    }

    @Override
    public void process(String key, String value) {
        String functionalKey = new FunctionalKeyExtractor<String, String>().extract(key, value);
        if (!hasSeen(functionalKey)) {
            context.forward(key, value, To.child("deduplicatedSink"));
            deduplicateStore.put(functionalKey, "1");
        } else {
            context.forward(key, value, To.child("discardDuplicatesSink"));
        }
    }

    private boolean hasSeen(String key) {
        return deduplicateStore.get(key) == null? false:true;
    }
}