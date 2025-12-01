package processor;


import org.example.model.Review;
import org.example.model.SentimentResult;
import org.example.processor.SentimentAnalyzer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UTSentimentAnalyzer {
    @Test
    void testAnalyzeMock() throws Exception {
        SentimentAnalyzer analyzer = new SentimentAnalyzer("dummy_key") {
            @Override
            public SentimentResult analyze(Review review) {
                return new SentimentResult("positive", 0.9);
            }
        };

        Review review = new Review("Good", 5);
        SentimentResult result = analyzer.analyze(review);
        assertEquals("positive", result.sentiment());
        assertEquals(0.9, result.score());
    }
}

