package com.idirze.kafka.deduplicator;

import com.idirze.kafka.deduplicator.partitioner.SourceTopicPartitionsRepartitioner;
import com.idirze.kafka.deduplicator.processor.DeduplicateProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
public class Driver {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-deduplicator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/rocksdb");

        Topology topology = new Topology();

        KeyValueBytesStoreSupplier storeSupplier = Stores
                .persistentKeyValueStore("deduplicate-store");

        StoreBuilder<KeyValueStore<String, String> >storeBuilder = Stores
                .keyValueStoreBuilder(storeSupplier,
                        String(),
                        String());

        topology.addSource("inputSource",
                String().deserializer(),
                String().deserializer(),
                "my-duplicates-input-topic")
                .addProcessor("deduplicate", () -> new DeduplicateProcessor(), "inputSource")
                .addSink("deduplicatedSink", "my-output-topic2", Serdes.String().serializer(),Serdes.String().serializer(),  "deduplicate")
                .addSink("discardDuplicatesSink", "my-error-topic", Serdes.String().serializer(),Serdes.String().serializer(),  "deduplicate")
                .addStateStore(storeBuilder, "deduplicate");

        KafkaStreams streaming = new KafkaStreams(topology, props);
        produceSmokeDate();

        streaming.start();
        consoleConsumer();
        Thread.sleep(1000000);


        log.info("Shutting down the Kafka Streams Application now");
        streaming.close();
    }

    private static void produceSmokeDate() {
        new Thread((() -> {
            Producer<String, String> p = createProducer();
            log.info("Sending ....");
            p.send(new ProducerRecord("my-duplicates-input-topic", "hello".hashCode()%1, "key1", "hello"));
            p.send(new ProducerRecord("my-duplicates-input-topic", "hello".hashCode()%1, "key1","hello"));
            p.send(new ProducerRecord("my-duplicates-input-topic", "hello".hashCode()%1, "key1","toto"));
            p.send(new ProducerRecord("my-duplicates-input-topic", "hello".hashCode()%1, "key1","titi"));
            p.flush();
            p.close();
        })).start();
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    private static void consoleConsumer() throws InterruptedException {

        log.info("Shutting up the Kafka Streams Application now");

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-deduplicator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        // previous requirements
        builder
                .stream("my-duplicates-input-topic", Consumed.with(String(), String()))
                .mapValues(v -> v)
                .print(Printed.<String, String>toSysOut().withLabel("inputSource"));

        builder
                .stream("my-output-topic2", Consumed.with(String(), String()))
                .print(Printed.<String, String>toSysOut().withLabel("deduplicatedSink"));

        builder
                .stream("my-error-topic", Consumed.with(String(), String()))
                .print(Printed.<String, String>toSysOut().withLabel("discardDuplicatesSink"));

        KafkaStreams streaming = new KafkaStreams(builder.build(), props);

        streaming.start();
        Thread.sleep(1000000);
        log.info("Shutting down the Kafka Streams Application now");
        streaming.close();

    }

}
