package com.example.AndesStay.dto;

import java.time.LocalDate;

public record ReservationEventDTO(
        Long reservationId,
        String guestName,
        Long unitId,
        LocalDate checkIn,
        LocalDate checkOut,
        String status,
        boolean guestAssigned
) {}
