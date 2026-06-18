package com.bank.dispatch_consumer.domain.model;

import lombok.*;

import java.time.Instant;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CardReplacementEvent {

    private String eventId;

    private String requestId;

    private String customerId;

    private String cardPANMasked;

    private String reasonCode;

    private String priority;

    private String branchCode;

    private String deliveryAddress;

    private Instant requestedAt;      // en dominio usamos Instant

    private Integer attemptNumber;    // 1 o 2

    private String correlationId;

    private String status;

}