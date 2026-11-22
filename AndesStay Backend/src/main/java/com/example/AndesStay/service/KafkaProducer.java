package com.example.AndesStay.service;

import com.example.AndesStay.dto.AuditEventDTO;
import com.example.AndesStay.dto.ReservationEventDTO;
import com.example.AndesStay.entity.AuditEvent;
import com.example.AndesStay.entity.Reservation;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    private final OutboxService outbox;

    public KafkaProducer(OutboxService outbox) {
        this.outbox = outbox;
    }

    public void sendReservationEvent(ReservationEventType type, Reservation reservation) {
        ReservationEventDTO dto = new ReservationEventDTO(
                reservation.getId(),
                reservation.getGuestName(),
                reservation.getUnitId(),
                reservation.getCheckIn(),
                reservation.getCheckOut(),
                reservation.getStatus(),
                reservation.isGuestAssigned()
        );
        outbox.enqueue("KAFKA", type.name(), dto);
    }

    public void sendReservationStatusChanged(Reservation reservation) {
        switch (reservation.getStatus()) {
            case "CONFIRMADA" ->
                    sendReservationEvent(ReservationEventType.RESERVATION_CONFIRMED, reservation);
            case "CANCELADA" ->
                    sendReservationEvent(ReservationEventType.RESERVATION_CANCELLED, reservation);
            default -> {
                // Sin evento de reserva para los demás estados.
            }
        }
    }

    public void sendAuditEvent(AuditEvent event) {
        AuditEventDTO dto = new AuditEventDTO(
                event.getId(),
                event.getAction(),
                event.getReservationId(),
                event.getDescription(),
                event.getEventDate(),
                event.getUser()
        );
        outbox.enqueue("KAFKA", "AUDIT_EVENT", dto);
    }
}