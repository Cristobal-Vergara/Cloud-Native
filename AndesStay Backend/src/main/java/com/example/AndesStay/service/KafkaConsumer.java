package com.example.AndesStay.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final InboxService inbox;
    private final KafkaEventProcessingService eventProcessingService;

    public KafkaConsumer(
            InboxService inbox,
            KafkaEventProcessingService eventProcessingService) {
        this.inbox = inbox;
        this.eventProcessingService = eventProcessingService;
    }

    // Listener antiguo: se conserva durante la migración.
    @KafkaListener(
            topics = "andesstay.events",
            groupId = "${spring.kafka.consumer.group-id:andesstay-group}")
    public void receiveEvent(String payload) {
        inbox.process("KAFKA", payload,
                () -> log.info("Evento Kafka antiguo registrado"));
    }

    // Consumidor de eventos de reservas.
    @KafkaListener(
            topics = "andesstay.reservations",
            groupId = "${andesstay.kafka.consumer.reservations-group-id:andesstay-reservations-group}",
            concurrency = "3")
    public void receiveReservationEvent(String payload) {
        eventProcessingService.processReservationEvent(payload);
    }

    // Consumidor independiente de eventos de auditoría.
    @KafkaListener(
            topics = "andesstay.audit",
            groupId = "${andesstay.kafka.consumer.audit-group-id:andesstay-audit-group}",
            concurrency = "3")
    public void receiveAuditEvent(String payload) {
        eventProcessingService.processAuditEvent(payload);
    }
}