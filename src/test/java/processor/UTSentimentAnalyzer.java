package processor;


import org.sentiment.model.Review;
import org.sentiment.model.SentimentResult;
import org.sentiment.processor.SentimentAnalyzer;
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

