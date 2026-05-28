package com.example.messageproducer.controller;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/quartz")
public class MessageProducerController {

    private final Scheduler scheduler;

    public MessageProducerController(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @GetMapping("/start")
    public ResponseEntity<String> startJob() {
        try {
            scheduler.start();
            return ResponseEntity.ok("Job avviato con successo");
        } catch (SchedulerException e) {
            return ResponseEntity.internalServerError().body("Errore nell'avvio del job: " + e.getMessage());
        }
    }

    @GetMapping("/stop")
    public ResponseEntity<String> stopJob() {
        try {
            scheduler.standby();
            return ResponseEntity.ok("Job fermato con successo");
        } catch (SchedulerException e) {
            return ResponseEntity.internalServerError().body("Errore nella fermata del job: " + e.getMessage());
        }
    }

    @GetMapping("/interval/{seconds}")
    public ResponseEntity<String> setInterval(@PathVariable int seconds) {
        try {
            TriggerKey triggerKey = new TriggerKey("kafkaTrigger", "group1");
            Trigger oldTrigger = scheduler.getTrigger(triggerKey);
            
            Trigger newTrigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey)
                .forJob(oldTrigger.getJobKey())
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                    .withIntervalInSeconds(seconds)
                    .repeatForever())
                .build();
            
            scheduler.rescheduleJob(triggerKey, newTrigger);
            
            return ResponseEntity.ok("Intervallo aggiornato a " + seconds + " secondi");
        } catch (SchedulerException e) {
            return ResponseEntity.internalServerError().body("Errore nell'aggiornamento dell'intervallo: " + e.getMessage());
        }
    }
}