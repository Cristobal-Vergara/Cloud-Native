package com.example.AndesStay.dto;

import java.time.LocalDateTime;

public record AuditEventDTO(
        Long auditEventId,
        String action,
        Long reservationId,
        String description,
        LocalDateTime eventDate,
        String user
) {}
