package com.example.AndesStay.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaEventProcessingServiceTests {

    @Mock
    private InboxService inbox;

    private KafkaEventProcessingService service;

    @BeforeEach
    void setUp() {
        service = new KafkaEventProcessingService(inbox);
    }

    @Test
    void processesReservationCreatedEvent() {
        String payload = reservationEvent("RESERVATION_CREATED", "CREADA");

        when(inbox.process(eq("KAFKA"), eq(payload), any(Runnable.class)))
                .thenReturn(true);

        assertTrue(service.processReservationEvent(payload));

        verify(inbox).process(eq("KAFKA"), eq(payload), any(Runnable.class));
    }

    @Test
    void processesReservationConfirmedEvent() {
        String payload = reservationEvent("RESERVATION_CONFIRMED", "CONFIRMADA");

        when(inbox.process(eq("KAFKA"), eq(payload), any(Runnable.class)))
                .thenReturn(true);

        assertTrue(service.processReservationEvent(payload));

        verify(inbox).process(eq("KAFKA"), eq(payload), any(Runnable.class));
    }

    @Test
    void processesReservationCancelledEvent() {
        String payload = reservationEvent("RESERVATION_CANCELLED", "CANCELADA");

        when(inbox.process(eq("KAFKA"), eq(payload), any(Runnable.class)))
                .thenReturn(true);

        assertTrue(service.processReservationEvent(payload));

        verify(inbox).process(eq("KAFKA"), eq(payload), any(Runnable.class));
    }

    @Test
    void processesAuditEvent() {
        String payload = auditEvent();

        when(inbox.process(eq("KAFKA"), eq(payload), any(Runnable.class)))
                .thenReturn(true);

        assertTrue(service.processAuditEvent(payload));

        verify(inbox).process(eq("KAFKA"), eq(payload), any(Runnable.class));
    }

    @Test
    void duplicateReservationEventIsReportedAsAlreadyProcessed() {
        String payload = reservationEvent("RESERVATION_CREATED", "CREADA");

        when(inbox.process(eq("KAFKA"), eq(payload), any(Runnable.class)))
                .thenReturn(false);

        assertFalse(service.processReservationEvent(payload));
    }

    @Test
    void rejectsInvalidUuidWithoutCallingInbox() {
        String payload = reservationEventWithId("invalid-id",
                "RESERVATION_CREATED", "CREADA");

        assertThrows(IllegalArgumentException.class,
                () -> service.processReservationEvent(payload));

        verifyNoInteractions(inbox);
    }

    @Test
    void rejectsUnknownReservationEventType() {
        String payload = reservationEvent("UNKNOWN_EVENT", "CREADA");

        assertThrows(IllegalArgumentException.class,
                () -> service.processReservationEvent(payload));

        verifyNoInteractions(inbox);
    }

    @Test
    void rejectsReservationStatusThatDoesNotMatchEventType() {
        String payload = reservationEvent("RESERVATION_CREATED", "CONFIRMADA");

        assertThrows(IllegalArgumentException.class,
                () -> service.processReservationEvent(payload));

        verifyNoInteractions(inbox);
    }

    @Test
    void rejectsInvalidReservationDates() {
        String payload = reservationEventWithDates(
                "RESERVATION_CREATED", "CREADA",
                "2026-11-05", "2026-11-01");

        assertThrows(IllegalArgumentException.class,
                () -> service.processReservationEvent(payload));

        verifyNoInteractions(inbox);
    }

    @Test
    void rejectsAuditMessageWithWrongType() {
        String payload = reservationEvent("RESERVATION_CREATED", "CREADA");

        assertThrows(IllegalArgumentException.class,
                () -> service.processAuditEvent(payload));

        verifyNoInteractions(inbox);
    }

    @Test
    void rejectsEmptyPayload() {
        assertThrows(IllegalArgumentException.class,
                () -> service.processReservationEvent(" "));

        verifyNoInteractions(inbox);
    }

    @Test
    void rejectsAuditWithoutRequiredFields() {
        String payload = """
                {
                  "id":"%s",
                  "type":"AUDIT_EVENT",
                  "occurredAt":"2026-11-22T01:00:00Z",
                  "data":{
                    "auditEventId":1,
                    "action":"",
                    "reservationId":11,
                    "description":"Test",
                    "eventDate":"2026-11-22T01:00:00",
                    "user":"admin"
                  }
                }
                """.formatted(UUID.randomUUID());

        assertThrows(IllegalArgumentException.class,
                () -> service.processAuditEvent(payload));

        verifyNoInteractions(inbox);
    }

    @Test
    void rejectsReservationEventWithoutId() {
        String payload = """
                {
                  "type":"RESERVATION_CREATED",
                  "occurredAt":"2026-11-22T01:00:00Z",
                  "data":{
                    "reservationId":11,
                    "guestName":"Prime jeje",
                    "unitId":2,
                    "checkIn":"2026-11-01",
                    "checkOut":"2026-11-05",
                    "status":"CREADA",
                    "guestAssigned":true
                  }
                }
                """;

        assertThrows(IllegalArgumentException.class,
                () -> service.processReservationEvent(payload));

        verifyNoInteractions(inbox);
    }

    @Test
    void rejectsReservationEventWithoutOccurredAt() {
        String payload = """
                {
                  "id":"%s",
                  "type":"RESERVATION_CREATED",
                  "data":{
                    "reservationId":11,
                    "guestName":"Prime jeje",
                    "unitId":2,
                    "checkIn":"2026-11-01",
                    "checkOut":"2026-11-05",
                    "status":"CREADA",
                    "guestAssigned":true
                  }
                }
                """.formatted(UUID.randomUUID());

        assertThrows(IllegalArgumentException.class,
                () -> service.processReservationEvent(payload));

        verifyNoInteractions(inbox);
    }
    private String reservationEvent(String type, String status) {
        return reservationEventWithId(UUID.randomUUID().toString(), type, status);
    }

    private String reservationEventWithId(String id, String type, String status) {
        return reservationEventWithDatesAndId(
                id, type, status, "2026-11-01", "2026-11-05");
    }

    private String reservationEventWithDates(
            String type, String status, String checkIn, String checkOut) {
        return reservationEventWithDatesAndId(
                UUID.randomUUID().toString(), type, status, checkIn, checkOut);
    }

    private String reservationEventWithDatesAndId(
            String id, String type, String status, String checkIn, String checkOut) {
        return """
                {
                  "id":"%s",
                  "type":"%s",
                  "occurredAt":"2026-11-22T01:00:00Z",
                  "data":{
                    "reservationId":11,
                    "guestName":"Prime jeje",
                    "unitId":2,
                    "checkIn":"%s",
                    "checkOut":"%s",
                    "status":"%s",
                    "guestAssigned":true
                  }
                }
                """.formatted(id, type, checkIn, checkOut, status);
    }

    private String auditEvent() {
        return """
                {
                  "id":"%s",
                  "type":"AUDIT_EVENT",
                  "occurredAt":"2026-11-22T01:00:00Z",
                  "data":{
                    "auditEventId":1,
                    "action":"RESERVA_CREADA",
                    "reservationId":11,
                    "description":"Se creó la reserva",
                    "eventDate":"2026-11-22T01:00:00",
                    "user":"admin"
                  }
                }
                """.formatted(UUID.randomUUID());
    }
}
