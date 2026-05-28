package com.example.messageproducer.quartz;

import com.example.messageproducer.model.Message;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.time.OffsetDateTime;
import java.util.UUID;

@Component
public class KafkaMessageJob implements Job {

    private final KafkaTemplate<String, Message> kafkaTemplate;

    @Value("${kafka.topic.name}")
    private String topic;

    public KafkaMessageJob(KafkaTemplate<String, Message> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        kafkaTemplate.send(topic, new Message(UUID.randomUUID().toString(), OffsetDateTime.now(), "messagecontent"));
        System.out.println("Sent message!");
    }
}