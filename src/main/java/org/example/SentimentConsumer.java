package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import okhttp3.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SentimentConsumer {

    private static final Logger log = LoggerFactory.getLogger(SentimentConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    public static void main(String[] args) throws Exception {
        String bootstrap = Utils.getEnvOrConfig("kafka.bootstrap.servers", "localhost:9092");
        String perplexityKey = Utils.getEnvOrConfig("perplexity.api.key", null);
        String esUrl = Utils.getEnvOrConfig("elasticsearch.url", "http://localhost:9200");
        String topic = "raw_topic";

        if (perplexityKey == null || perplexityKey.isBlank()) {
            log.error("Perplexity API key not set. Exiting.");
            return;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sentiment-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        OkHttpClient httpClient = new OkHttpClient();
        log.info("SentimentConsumer started. Listening to Kafka topic: {}", topic);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                String reviewJson = record.value();
                String reviewText = extractReviewText(reviewJson);
                int starRating = extractStarRating(reviewJson);

                if (reviewText == null || reviewText.isBlank()) continue;

                log.info("Sending full review with star rating {}: {}", starRating, reviewText);

                // Build API request body
                ObjectNode requestBody = MAPPER.createObjectNode();
                requestBody.put("model", "sonar-pro");

                ArrayNode messages = MAPPER.createArrayNode();
                ObjectNode systemMessage = MAPPER.createObjectNode();
                systemMessage.put("role", "system");
                systemMessage.put("content", "You are a sentiment analysis assistant. Analyze the full review text and star rating, and return JSON as {sentiment: positive/neutral/negative, score: 0-1}.No other text or details");

                ObjectNode userMessage = MAPPER.createObjectNode();
                userMessage.put("role", "user");
                userMessage.put("content", "Review text: " + reviewText.replace("\n", " ") + "\nStar rating: " + starRating);

                messages.add(systemMessage);
                messages.add(userMessage);
                requestBody.set("messages", messages);

                String jsonRequestBody = requestBody.toString();
                RequestBody body = RequestBody.create(jsonRequestBody, JSON);

                Request request = new Request.Builder()
                        .url("https://api.perplexity.ai/chat/completions")
                        .header("Authorization", "Bearer " + perplexityKey)
                        .post(body)
                        .build();

                String finalSentiment = "unknown";
                double score = 0.0;

                try (Response response = httpClient.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        log.error("Perplexity API failed with status code: {}", response.code());
                        log.error("Response message: {}", response.body() != null ? response.body().string() : "null");
                    } else {
                        String respString = response.body().string();
                        JsonNode respJson = MAPPER.readTree(respString);
                        String assistantText = "";
                        if (respJson.has("choices") && respJson.get("choices").size() > 0) {
                            JsonNode msg = respJson.get("choices").get(0).get("message");
                            if (msg != null && msg.has("content")) assistantText = msg.get("content").asText();
                        }
                        try {
                            JsonNode parsed = MAPPER.readTree(assistantText);
                            if (parsed.has("sentiment")) finalSentiment = parsed.get("sentiment").asText("");
                            if (parsed.has("score")) score = parsed.get("score").asDouble(0.0);
                        } catch (Exception ex) {
                            finalSentiment = assistantText.trim();
                        }
                        log.info("Perplexity API returned sentiment={} score={}", finalSentiment, score);
                    }
                } catch (Exception e) {
                    log.error("Exception while calling Perplexity API", e);
                }

                // Push to Elasticsearch
                ObjectNode doc = MAPPER.createObjectNode();
                doc.put("review_text", reviewText);
                doc.put("sentiment", finalSentiment);
                doc.put("score", score);
                doc.put("star_rating", starRating);
                doc.put("kafka_partition", record.partition());
                doc.put("kafka_offset", record.offset());
                doc.put("timestamp", System.currentTimeMillis());

                String docJson = doc.toString();
                RequestBody esBody = RequestBody.create(docJson, JSON);
                Request esRequest = new Request.Builder()
                        .url(esUrl + "/yelp/_doc/" + record.offset())
                        .post(esBody)
                        .build();

                try (Response esResp = httpClient.newCall(esRequest).execute()) {
                    if (!esResp.isSuccessful()) {
                        log.error("Elasticsearch push failed: {} {}", esResp.code(), esResp.body() != null ? esResp.body().string() : "null");
                    } else {
                        log.info("Indexed offset={} sentiment={} score={} star_rating={}",
                                record.offset(), finalSentiment, score, starRating);
                    }
                } catch (Exception e) {
                    log.error("Exception while pushing to Elasticsearch for offset={}", record.offset(), e);
                }
            }
        }
    }

    private static String extractReviewText(String reviewJson) {
        try {
            JsonNode n = MAPPER.readTree(reviewJson);
            if (n.has("text")) return n.get("text").asText("");
            return reviewJson;
        } catch (Exception e) {
            return reviewJson;
        }
    }

    private static int extractStarRating(String reviewJson) {
        try {
            JsonNode n = MAPPER.readTree(reviewJson);
            if (n.has("stars")) return n.get("stars").asInt(0);
        } catch (Exception e) {
            // ignore
        }
        return 0;
    }
}
