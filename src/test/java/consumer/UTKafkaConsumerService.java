package consumer;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.example.consumer.KafkaConsumerService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UTKafkaConsumerService {
    @Test
    void testPollClose() {
        KafkaConsumerService consumer = new KafkaConsumerService("localhost:9092", "raw_topic", "group1") {
            @Override
            public ConsumerRecords<String, String> poll(long timeoutMs) {
                return new ConsumerRecords<>(java.util.Collections.emptyMap());
            }
        };
        assertNotNull(consumer.poll(1000));
        consumer.close();
    }
}

