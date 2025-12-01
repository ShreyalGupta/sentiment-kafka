package org.example.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.Review;

public class ReviewParser {

    private static final ObjectMapper mapper = new ObjectMapper();

    public Review parse(String json) {
        try {
            if (!json.trim().startsWith("{")) {
                return null;
            }
            JsonNode node = mapper.readTree(json);

            String text = node.has("text") ? node.get("text").asText("") : "";
            int stars = node.has("stars") ? node.get("stars").asInt(0) : 0;

            if (text.isBlank()) return null;

            return new Review(text, stars);

        } catch (Exception e) {
            return null;
        }
    }
}
