package org.sentiment.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerService {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaProducerService(String bootstrapServers, String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
    }

    public KafkaProducerService(KafkaProducer<String, String> mockProducer, String topic) {
        this.producer = mockProducer;
        this.topic = topic;
    }

    public void send(String message) {
        producer.send(new ProducerRecord<>(topic, null, message));
    }

    public void close() { producer.close(); }
}
