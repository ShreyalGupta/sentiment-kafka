package org.example.elasticsearch;

import okhttp3.*;
import org.example.model.Review;
import org.example.model.SentimentResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class ElasticSearchService {
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final OkHttpClient client = new OkHttpClient();
    private final String esUrl;

    public ElasticSearchService(String esUrl) { this.esUrl = esUrl; }

    public void indexReview(long id, Review review, SentimentResult sentiment) throws IOException {
        ObjectNode doc = MAPPER.createObjectNode();
        doc.put("review_text", review.getText());
        doc.put("sentiment", sentiment.sentiment());
        doc.put("score", sentiment.score());
        doc.put("star_rating", review.getStars());
        doc.put("timestamp", System.currentTimeMillis());

        RequestBody body = RequestBody.create(doc.toString(), JSON);
        Request request = new Request.Builder()
                .url(esUrl + "/yelp/_doc/" + id)
                .post(body)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Elasticsearch indexing failed: " + response.code());
            }
        }
    }
}

