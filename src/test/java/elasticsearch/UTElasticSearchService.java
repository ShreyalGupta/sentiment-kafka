package elasticsearch;


import org.example.elasticsearch.ElasticSearchService;
import org.example.model.Review;
import org.example.model.SentimentResult;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UTElasticSearchService {
    @Test
    void testIndexReview() throws Exception {
        ElasticSearchService es = new ElasticSearchService("http://localhost:9200") {
            @Override
            public void indexReview(long id, Review review, SentimentResult sentiment) {
                assertNotNull(review);
                assertNotNull(sentiment);
            }
        };

        es.indexReview(1, new Review("Nice", 5), new SentimentResult("positive", 0.8));
    }
}
