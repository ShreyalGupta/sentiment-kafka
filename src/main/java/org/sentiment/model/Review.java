package org.sentiment.model;

public class Review {
    private String text;
    private int stars;

    public Review(String text, int stars) {
        this.text = text;
        this.stars = stars;
    }

    public String getText() { return text; }
    public int getStars() { return stars; }
}

