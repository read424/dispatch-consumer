package com.bank.dispatch_consumer.domain.dto;

import com.bank.events.CardReplacementEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotDto {

    private String eventId;
    private String requestId;
    private String customerId;
    private String cardPANMasked;
    private String reasonCode;
    private String priority;
    private String branchCode;
    private String deliveryAddress;
    private Instant requestedAt;
    private Integer attemptNumber;
    private String correlationId;
    private String status;

    public static SnapshotDto fromAvroEvent(CardReplacementEvent ev) {
        if (ev == null) return null;

        return SnapshotDto.builder()
                .eventId(ev.getEventId() != null ? ev.getEventId().toString() : null)
                .requestId(ev.getRequestId() != null ? ev.getRequestId().toString() : null)
                .customerId(ev.getCustomerId() != null ? ev.getCustomerId().toString() : null)
                .cardPANMasked(ev.getCardPANMasked() != null ? ev.getCardPANMasked().toString() : null)
                .reasonCode(ev.getReasonCode() != null ? ev.getReasonCode().toString() : null)
                .priority(ev.getPriority() != null ? ev.getPriority().toString() : null)
                .branchCode(ev.getBranchCode() != null ? ev.getBranchCode().toString() : null)
                .deliveryAddress(ev.getDeliveryAddress() != null ? ev.getDeliveryAddress().toString() : null)
                .requestedAt(ev.getRequestedAt())
                .attemptNumber(ev.getAttemptNumber())
                .correlationId(ev.getCorrelationId() != null ? ev.getCorrelationId().toString() : null)
                .status(ev.getStatus() != null ? ev.getStatus().toString() : null)
                .build();
    }
}
