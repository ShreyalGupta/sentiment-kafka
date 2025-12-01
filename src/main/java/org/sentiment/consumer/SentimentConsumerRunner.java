package org.sentiment.consumer;


import org.sentiment.model.Review;
import org.sentiment.model.SentimentResult;
import org.sentiment.processor.*;
import org.sentiment.elasticsearch.ElasticSearchService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class SentimentConsumerRunner {

    public static void main(String[] args) throws Exception {

        String bootstrap = "localhost:9092";
        String topic = "raw_topic";
        String perplexityKey = System.getenv("PERPLEXITY_API_KEY");
        String esUrl = "http://localhost:9200";

        KafkaConsumerService consumer = new KafkaConsumerService(bootstrap, topic, "sentiment-group");
        SentimentAnalyzer analyzer = new SentimentAnalyzer(perplexityKey);
        ElasticSearchService es = new ElasticSearchService(esUrl);
        ReviewParser parser = new ReviewParser();

        System.out.println("Sentiment consumer started...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                // Step 1: Parse JSON → Review object
                Review review = parser.parse(record.value());
                if (review == null) continue;

                // Step 2: Get sentiment
                SentimentResult result = analyzer.analyze(review);

                // Step 3: Push to Elasticsearch
                es.indexReview(record.offset(), review, result);

                System.out.println("Indexed: " + record.offset());
            }
        }
    }
}
