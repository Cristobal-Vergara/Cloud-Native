package com.example.AndesStay.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
@EnableScheduling
public class MessagingConfig {

    @Bean
    public NewTopic reservationTopic() {
        return TopicBuilder.name("andesstay.events")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic reservationsBusinessTopic() {
        return TopicBuilder.name("andesstay.reservations")
                .partitions(3)
                .replicas(1)
                .configs(Map.of(
                        "retention.ms", "604800000",
                        "cleanup.policy", "delete"
                ))
                .build();
    }

    @Bean
    public NewTopic auditBusinessTopic() {
        return TopicBuilder.name("andesstay.audit")
                .partitions(3)
                .replicas(1)
                .configs(Map.of(
                        "retention.ms", "2592000000",
                        "cleanup.policy", "delete"
                ))
                .build();
    }

    @Bean
    public NewTopic legacyDeadLetterTopic() {
        return TopicBuilder.name("andesstay.events.DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic reservationsDeadLetterTopic() {
        return TopicBuilder.name("andesstay.reservations.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic auditDeadLetterTopic() {
        return TopicBuilder.name("andesstay.audit.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) -> new TopicPartition(
                                record.topic() + ".DLT",
                                record.partition()
                        )
                );

        recoverer.setFailIfSendResultIsError(true);

        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(1000L, 3L)
        );

        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        return handler;
    }
}
