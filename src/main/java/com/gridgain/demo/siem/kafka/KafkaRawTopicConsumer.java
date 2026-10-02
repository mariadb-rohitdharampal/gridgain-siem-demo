package com.gridgain.demo.siem.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public final class KafkaRawTopicConsumer implements KafkaRawEventConsumer {
    private static final String CLIENT_ID = "gridgain-siem-raw-consumer";

    private final KafkaLogEventJsonCodec codec;
    private final Consumer<String, String> consumer;

    public KafkaRawTopicConsumer(KafkaIntegrationConfig config) {
        this(config, new KafkaLogEventJsonCodec(), new KafkaConsumer<>(consumerProperties(config)));
    }

    KafkaRawTopicConsumer(
            KafkaIntegrationConfig config,
            KafkaLogEventJsonCodec codec,
            Consumer<String, String> consumer
    ) {
        this.codec = codec;
        this.consumer = consumer;
        this.consumer.subscribe(config.rawTopicNames().values());
    }

    @Override
    public KafkaRawEventBatch poll(Duration timeout) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        ArrayList<KafkaLogEventRecord> decodedRecords = new ArrayList<>(records.count());
        Map<String, Integer> consumedTopicCounts = new LinkedHashMap<>();
        int malformedRecords = 0;

        for (ConsumerRecord<String, String> record : records) {
            consumedTopicCounts.merge(record.topic(), 1, Integer::sum);
            try {
                decodedRecords.add(new KafkaLogEventRecord(
                        record.topic(),
                        record.key(),
                        record.partition(),
                        record.offset(),
                        codec.fromJson(record.value())
                ));
            } catch (IllegalArgumentException ex) {
                malformedRecords++;
                System.err.println("Skipping malformed Kafka log event at %s-%d offset %d: %s"
                        .formatted(record.topic(), record.partition(), record.offset(), ex.getMessage()));
            }
        }

        return new KafkaRawEventBatch(decodedRecords, consumedTopicCounts, malformedRecords);
    }

    @Override
    public void commit() {
        consumer.commitSync();
    }

    @Override
    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public void close() {
        consumer.close();
    }

    private static Properties consumerProperties(KafkaIntegrationConfig config) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.consumerGroupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetReset());
        return properties;
    }
}
