package com.bank.dispatch_consumer.domain.entity;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("card_replacements")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CardReplacementEntity {

    @Id
    private String requestId;
    private String customerId;
    private String branchCode;
    private String deliveryAddress;
    private long requestedAt;  // epoch millis
    private int attemptNumber;
    private String correlationId;
    private String status;
    private Long receivedAt;
    private Long processedAt;
    private String rawPayload;
    private String errorMessage;

}