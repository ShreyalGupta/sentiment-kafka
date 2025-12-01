package processor;


import org.example.model.Review;
import org.example.processor.ReviewParser;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class UTReviewParser {
    @Test
    void parseValidJson() {
        ReviewParser parser = new ReviewParser();
        String json = "{\"text\":\"Nice\",\"stars\":4}";
        Review review = parser.parse(json);
        assertNotNull(review);
        assertEquals("Nice", review.getText());
        assertEquals(4, review.getStars());
    }

    @Test
    void parseInvalidJson() {
        ReviewParser parser = new ReviewParser();
        assertNull(parser.parse("invalid json"));
    }
}

