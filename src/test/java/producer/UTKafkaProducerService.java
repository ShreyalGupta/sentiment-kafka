package producer;

import org.sentiment.producer.KafkaProducerService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static org.mockito.ArgumentMatchers.any;

public class UTKafkaProducerService {

    @Test
    void testSendClose() {
        // Mock KafkaProducer
        KafkaProducer<String,String> mockKafkaProducer = Mockito.mock(KafkaProducer.class);

        // Use TESTING constructor here
        KafkaProducerService service = new KafkaProducerService(mockKafkaProducer, "raw_topic");

        service.send("{\"text\":\"Good\",\"stars\":5}");
        service.close();

        // Verify behavior
        Mockito.verify(mockKafkaProducer).send(any(ProducerRecord.class));
        Mockito.verify(mockKafkaProducer).close();
    }
}
