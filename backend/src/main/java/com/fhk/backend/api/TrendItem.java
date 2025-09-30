package com.fhk.backend.api;

public record TrendItem(String id, String title, double score, String sentiment, String topicId){}