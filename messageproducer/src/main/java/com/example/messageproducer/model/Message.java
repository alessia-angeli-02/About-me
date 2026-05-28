package com.example.messageproducer.model;

import java.time.OffsetDateTime;

public record Message(String id, OffsetDateTime timestamp, String content) {
}
