package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class YelpProducer {
    public static void main(String[] args) throws Exception {
        String bootstrap = Utils.getEnvOrConfig("kafka.bootstrap.servers", "localhost:9092");
        String yelpFile = Utils.getEnvOrConfig("yelp.file.path", "C:\\Users\\shrey\\Downloads\\archive\\yelp_academic_dataset_review.json");
        String topic = "raw_topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader br = new BufferedReader(new FileReader(yelpFile))) {

            String line;
            long count = 0;
            while ((line = br.readLine()) != null) {
                // each line in Yelp reviews file is a JSON object; send entire line as value
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, line);
                producer.send(record);
                count++;
                if (count % 1000 == 0) {
                    System.out.println("Sent " + count + " reviews");
                }
                Thread.sleep(10); // to throttle - adjust or remove if desired
            }
            producer.flush();
            System.out.println("Done. Sent total " + count + " messages to topic " + topic);
        }
    }
}

