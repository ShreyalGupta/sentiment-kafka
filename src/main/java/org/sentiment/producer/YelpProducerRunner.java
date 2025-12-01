package org.sentiment.producer;

import java.io.BufferedReader;
import java.io.FileReader;

public class YelpProducerRunner {

    public static void main(String[] args) throws Exception {
        String bootstrap = "localhost:9092";
        String topic = "raw_topic";
        String yelpFile = "C:\\Users\\shrey\\Downloads\\archive\\yelp_academic_dataset_review.json";

        KafkaProducerService producer = new KafkaProducerService(bootstrap, topic);

        try (BufferedReader br = new BufferedReader(new FileReader(yelpFile))) {
            String line;
            long count = 0;

            while ((line = br.readLine()) != null) {
                producer.send(line);
                count++;

                if (count % 1000 == 0) {
                    System.out.println("Sent " + count + " messages");
                }
                Thread.sleep(10);
            }
        }

        producer.close();
        System.out.println("Finished sending messages!");
    }
}

