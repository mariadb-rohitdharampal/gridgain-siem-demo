package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public final class KafkaRawEventProducer implements KafkaRawEventSink {
    private static final String CLIENT_ID = "gridgain-siem-raw-producer";

    private final KafkaIntegrationConfig config;
    private final KafkaLogEventJsonCodec codec;
    private final Producer<String, String> producer;

    public KafkaRawEventProducer(KafkaIntegrationConfig config) {
        this(config, new KafkaLogEventJsonCodec(), new KafkaProducer<>(producerProperties(config)));
    }

    KafkaRawEventProducer(
            KafkaIntegrationConfig config,
            KafkaLogEventJsonCodec codec,
            Producer<String, String> producer
    ) {
        this.config = config;
        this.codec = codec;
        this.producer = producer;
    }

    @Override
    public void publish(LogEvent event) {
        try {
            producer.send(toProducerRecord(event, config, codec)).get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while producing Kafka event", ex);
        } catch (ExecutionException ex) {
            throw new IllegalStateException("Failed to produce Kafka event to " + topicFor(event, config), ex);
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }

    public static ProducerRecord<String, String> toProducerRecord(
            LogEvent event,
            KafkaIntegrationConfig config,
            KafkaLogEventJsonCodec codec
    ) {
        return new ProducerRecord<>(
                topicFor(event, config),
                keyFor(event),
                codec.toJson(event)
        );
    }

    public static String topicFor(LogEvent event, KafkaIntegrationConfig config) {
        return config.rawTopicName(event.sourceType());
    }

    public static String keyFor(LogEvent event) {
        return event.sourceType().name() + "|" + event.reductionKey();
    }

    private static Properties producerProperties(KafkaIntegrationConfig config) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }
}
