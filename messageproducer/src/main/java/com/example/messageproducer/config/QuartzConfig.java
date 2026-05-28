package com.example.messageproducer.config;

import com.example.messageproducer.quartz.KafkaMessageJob;
import org.quartz.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

@Configuration
public class QuartzConfig {

    private KafkaMessageJob kafkaMessageJob;
    private AutowiringSpringBeanJobFactory jobFactory;

    public QuartzConfig(KafkaMessageJob kafkaMessageJob, AutowiringSpringBeanJobFactory jobFactory) {
        this.kafkaMessageJob = kafkaMessageJob;
        this.jobFactory = jobFactory;
    }

    @Bean
    public JobDetail kafkaJobDetail() {
        return JobBuilder.newJob(KafkaMessageJob.class)
                .withIdentity("kafkaJob", "group1")
                .storeDurably()
                .build();
    }

    @Bean
    public Trigger kafkaJobTrigger() {
        SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder.simpleSchedule()
                .withIntervalInSeconds(1) // Invio messaggi ogni 60 secondi
                .repeatForever();

        return TriggerBuilder.newTrigger()
                .forJob(kafkaJobDetail())
                .withIdentity("kafkaTrigger", "group1")
                .withSchedule(scheduleBuilder)
                .build();
    }

    @Bean
    public SchedulerFactoryBean schedulerFactoryBean(Trigger kafkaJobTrigger, JobDetail kafkaJobDetail) {
        SchedulerFactoryBean schedulerFactoryBean = new SchedulerFactoryBean();
        schedulerFactoryBean.setJobFactory(jobFactory); 
        schedulerFactoryBean.setJobDetails(kafkaJobDetail);
        schedulerFactoryBean.setTriggers(kafkaJobTrigger);
        return schedulerFactoryBean;
    }

    @Bean
    public Scheduler scheduler(SchedulerFactoryBean schedulerFactoryBean) throws SchedulerException {
        return schedulerFactoryBean.getScheduler();
    }
}
