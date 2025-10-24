package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import okhttp3.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class SentimentConsumer {

    private static final Logger log = LoggerFactory.getLogger(SentimentConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private static final OkHttpClient httpClient = new OkHttpClient.Builder()
            .callTimeout(Duration.ofSeconds(60))
            .build();

    private static final String OPENAI_API_KEY = System.getenv("OPENAI_API_KEY");
    private static final int BATCH_SIZE = 5; // number of messages to send per API call

    public static void main(String[] args) throws Exception {
        String bootstrap = Utils.getEnvOrConfig("kafka.bootstrap.servers", "localhost:9092");
        String esUrl = Utils.getEnvOrConfig("elasticsearch.url", "http://localhost:9200");
        String topic = "raw_topic";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sentiment-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        List<ConsumerRecord<String, String>> batch = new ArrayList<>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                batch.add(record);
                if (batch.size() >= BATCH_SIZE) {
                    processBatch(batch, esUrl);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                processBatch(batch, esUrl);
                batch.clear();
            }
        }
    }

    private static void processBatch(List<ConsumerRecord<String, String>> batch, String esUrl) {
        try {
            StringBuilder promptBuilder = new StringBuilder("Classify the sentiment (positive, neutral, negative) of the following reviews:\n");
            Map<Integer, ConsumerRecord<String, String>> offsetMap = new HashMap<>();
            int idx = 1;
            for (ConsumerRecord<String, String> record : batch) {
                JsonNode reviewNode = MAPPER.readTree(record.value());
                String text = reviewNode.path("text").asText("");
                if (text.isBlank()) continue;

                promptBuilder.append(idx).append(". ").append(text).append("\n");
                offsetMap.put(idx, record);
                idx++;
            }

            String prompt = promptBuilder.toString();

            JsonNode requestBody = MAPPER.createObjectNode()
                    .put("model", "gpt-3.5-turbo")
                    .putArray("messages")
                    .add(MAPPER.createObjectNode()
                            .put("role", "user")
                            .put("content", prompt));

            RequestBody body = RequestBody.create(MAPPER.writeValueAsBytes(requestBody), MediaType.get("application/json"));

            Request request = new Request.Builder()
                    .url("https://api.openai.com/v1/chat/completions")
                    .header("Authorization", "Bearer " + OPENAI_API_KEY)
                    .post(body)
                    .build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful() || response.body() == null) {
                    log.warn("OpenAI API failed: {}", response);
                    return;
                }

                String respString = response.body().string();
                JsonNode respJson = MAPPER.readTree(respString);
                String content = respJson.path("choices").get(0).path("message").path("content").asText();

                // Split results by line (assuming model responds 1:1 in order)
                String[] lines = content.split("\\n");
                for (String line : lines) {
                    line = line.toLowerCase();
                    int dotIdx = line.indexOf(".");
                    if (dotIdx > 0) {
                        try {
                            int reviewNum = Integer.parseInt(line.substring(0, dotIdx).trim());
                            String sentiment = line.substring(dotIdx + 1).trim();
                            if (sentiment.contains("positive")) sentiment = "positive";
                            else if (sentiment.contains("negative")) sentiment = "negative";
                            else sentiment = "neutral";

                            ConsumerRecord<String, String> record = offsetMap.get(reviewNum);
                            JsonNode reviewNode = MAPPER.readTree(record.value());
                            String reviewText = reviewNode.path("text").asText("");
                            int stars = reviewNode.path("stars").asInt(0);
                            double score = sentiment.equals("positive") ? 0.9 :
                                    sentiment.equals("neutral") ? 0.5 : 0.2;

                            // Save to Elasticsearch
                            JsonNode doc = MAPPER.createObjectNode()
                                    .put("review_text", reviewText)
                                    .put("stars", stars)
                                    .put("sentiment", sentiment)
                                    .put("score", score)
                                    .put("kafka_partition", record.partition())
                                    .put("kafka_offset", record.offset())
                                    .put("timestamp", System.currentTimeMillis());

                            RequestBody esBody = RequestBody.create(MAPPER.writeValueAsBytes(doc), MediaType.get("application/json"));
                            Request esReq = new Request.Builder()
                                    .url(esUrl + "/yelp/_doc")
                                    .post(esBody)
                                    .build();

                            try (Response esResp = httpClient.newCall(esReq).execute()) {
                                if (!esResp.isSuccessful()) {
                                    log.error("ES push failed: {} {}", esResp.code(), esResp.body().string());
                                }
                            }

                            log.info("Processed review offset={} sentiment={} score={}", record.offset(), sentiment, score);
                        } catch (NumberFormatException ignored) {}
                    }
                }

            }

        } catch (Exception e) {
            log.error("Error processing batch", e);
        }
    }
}
