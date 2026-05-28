package com.example.demo.model;

import java.time.OffsetDateTime;

public class Message {
    String id;
    Double timestamp;
    String content;

    public String getId() {
        return id;
    }

    public Double getTimestamp() {
        return timestamp;
    }

    public String getContent() {
        return content;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTimestamp(Double timestamp) {
        this.timestamp = timestamp;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Message(String id, Double timestamp, String content) {
        this.id = id;
        this.timestamp = timestamp;
        this.content = content;
    }

    public Message() {
    }
}
