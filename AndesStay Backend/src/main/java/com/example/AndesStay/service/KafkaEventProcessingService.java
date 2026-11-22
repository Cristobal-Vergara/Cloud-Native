package com.example.AndesStay.service;

import com.example.AndesStay.dto.AuditEventDTO;
import com.example.AndesStay.dto.ReservationEventDTO;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;

@Service
public class KafkaEventProcessingService {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaEventProcessingService.class);

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final InboxService inbox;

    public KafkaEventProcessingService(InboxService inbox) {
        this.inbox = inbox;
    }

    public boolean processReservationEvent(String payload) {
        JsonNode envelope = validateEnvelope(payload);
        String type = envelope.path("type").asString();

        ReservationEventType eventType;
        try {
            eventType = ReservationEventType.valueOf(type);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "Tipo de evento de reserva no admitido: " + type, exception);
        }

        JsonNode data = envelope.get("data");
        if (data == null || !data.isObject() || !data.has("guestAssigned")
                || !data.get("guestAssigned").isBoolean()) {
            throw new IllegalArgumentException(
                    "El contenido del evento de reserva no tiene la estructura requerida");
        }

        ReservationEventDTO dto =
                JSON.readValue(data.toString(), ReservationEventDTO.class);

        validateReservation(dto, eventType);

        return inbox.process("KAFKA", payload, () ->
                log.info("Evento Kafka de reserva procesado: tipo={}, reservaId={}",
                        type, dto.reservationId()));
    }

    public boolean processAuditEvent(String payload) {
        JsonNode envelope = validateEnvelope(payload);
        String type = envelope.path("type").asString();

        if (!"AUDIT_EVENT".equals(type)) {
            throw new IllegalArgumentException(
                    "Tipo de evento de auditoría no admitido: " + type);
        }

        JsonNode data = envelope.get("data");
        if (data == null || !data.isObject()) {
            throw new IllegalArgumentException(
                    "El contenido del evento de auditoría no es válido");
        }

        AuditEventDTO dto =
                JSON.readValue(data.toString(), AuditEventDTO.class);

        validateAudit(dto);

        return inbox.process("KAFKA", payload, () ->
                log.info("Evento Kafka de auditoría procesado: acción={}, auditEventId={}",
                        dto.action(), dto.auditEventId()));
    }

    private JsonNode validateEnvelope(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("El mensaje Kafka está vacío");
        }

        JsonNode envelope = JSON.readTree(payload);

        if (envelope == null || !envelope.isObject()) {
            throw new IllegalArgumentException("El sobre del evento Kafka no es válido");
        }

        String id = envelope.path("id").asString();
        String type = envelope.path("type").asString();
        String occurredAt = envelope.path("occurredAt").asString();

        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("El ID del evento Kafka es obligatorio");
        }

        try {
            UUID.fromString(id);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "El ID del evento Kafka no es un UUID válido", exception);
        }

        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("El tipo del evento Kafka es obligatorio");
        }

        if (occurredAt == null || occurredAt.isBlank()) {
            throw new IllegalArgumentException(
                    "La fecha occurredAt del evento Kafka es obligatoria");
        }

        try {
            Instant.parse(occurredAt);
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException(
                    "La fecha occurredAt del evento no es válida", exception);
        }

        JsonNode data = envelope.get("data");
        if (data == null || data.isNull() || !data.isObject()) {
            throw new IllegalArgumentException(
                    "El campo data del evento Kafka es obligatorio");
        }

        return envelope;
    }

    private void validateReservation(
            ReservationEventDTO dto,
            ReservationEventType eventType) {

        if (dto.reservationId() == null || dto.reservationId() <= 0
                || dto.unitId() == null || dto.unitId() <= 0) {
            throw new IllegalArgumentException("La reserva debe tener IDs válidos");
        }

        if (dto.guestName() == null || dto.guestName().isBlank()) {
            throw new IllegalArgumentException("El nombre del huésped es obligatorio");
        }

        if (dto.checkIn() == null || dto.checkOut() == null
                || !dto.checkOut().isAfter(dto.checkIn())) {
            throw new IllegalArgumentException("Las fechas de la reserva no son válidas");
        }

        if (!eventType.status().equals(dto.status())) {
            throw new IllegalArgumentException(
                    "El estado de la reserva no coincide con el tipo de evento");
        }
    }

    private void validateAudit(AuditEventDTO dto) {
        if (dto.auditEventId() == null || dto.auditEventId() <= 0) {
            throw new IllegalArgumentException("El ID del evento de auditoría no es válido");
        }

        if (dto.action() == null || dto.action().isBlank()
                || dto.description() == null || dto.description().isBlank()
                || dto.user() == null || dto.user().isBlank()
                || dto.eventDate() == null) {
            throw new IllegalArgumentException(
                    "El evento de auditoría contiene campos obligatorios vacíos");
        }

        if (dto.reservationId() != null && dto.reservationId() <= 0) {
            throw new IllegalArgumentException("El ID de reserva de auditoría no es válido");
        }
    }
}
