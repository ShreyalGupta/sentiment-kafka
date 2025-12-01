package org.example.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.*;
import org.example.model.Review;
import org.example.model.SentimentResult;

import java.io.IOException;

public class SentimentAnalyzer {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private final OkHttpClient client = new OkHttpClient();
    private final String apiKey;

    public SentimentAnalyzer(String apiKey) {
        this.apiKey = apiKey;
    }

    public SentimentResult analyze(Review review) throws IOException {
        // Build JSON body for API request
        ObjectNode requestBody = MAPPER.createObjectNode();
        requestBody.put("model", "sonar-pro");

        ArrayNode messages = MAPPER.createArrayNode();
        ObjectNode systemMsg = MAPPER.createObjectNode();
        systemMsg.put("role", "system");
        systemMsg.put("content", "Analyze review and return JSON {sentiment, score}");

        ObjectNode userMsg = MAPPER.createObjectNode();
        userMsg.put("role", "user");
        userMsg.put("content", "Review text: " + review.getText() + "\nStar rating: " + review.getStars());

        messages.add(systemMsg);
        messages.add(userMsg);
        requestBody.set("messages", messages);

        RequestBody body = RequestBody.create(requestBody.toString(), JSON);
        Request request = new Request.Builder()
                .url("https://api.perplexity.ai/chat/completions")
                .header("Authorization", "Bearer " + apiKey)
                .post(body)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) throw new IOException("API call failed: " + response);

            String respStr = response.body().string();
            JsonNode respJson = MAPPER.readTree(respStr);

            String sentiment = "unknown";
            double score = 0.0;

            // Extract content from API response
            if (respJson.has("choices") && respJson.get("choices").size() > 0) {
                JsonNode content = respJson.get("choices").get(0).get("message").get("content");
                if (content != null) {
                    String contentText = content.asText();

                    // --- CLEAN THE RESPONSE ---
                    // Remove markdown ```json or ``` blocks
                    contentText = contentText.trim()
                            .replaceAll("^```json", "")
                            .replaceAll("^```", "")
                            .replaceAll("```$", "")
                            .trim();

                    // Parse clean JSON
                    JsonNode parsed = MAPPER.readTree(contentText);
                    sentiment = parsed.has("sentiment") ? parsed.get("sentiment").asText() : "unknown";
                    score = parsed.has("score") ? parsed.get("score").asDouble() : 0.0;
                }
            }

            return new SentimentResult(sentiment, score);
        }
    }
}
