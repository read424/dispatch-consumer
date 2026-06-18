package com.bank.dispatch_consumer.domain.mapper;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.events.CardReplacementEvent;
import org.apache.avro.generic.GenericRecord;
import org.mapstruct.Mapper;

import java.time.Instant;

@Mapper(componentModel = "spring")
public interface EntityMapper {

    // Avro GenericRecord -> Modelo
    default CardReplacementEvent toEvent(GenericRecord gr) {

        if (gr == null) return null;

        CardReplacementEvent e = new CardReplacementEvent();
        // Avro generated setters accept CharSequence for strings -> casteamos a CharSequence
        e.setEventId((java.lang.CharSequence) gr.get("eventId"));
        e.setRequestId((java.lang.CharSequence) gr.get("requestId"));
        e.setCustomerId((java.lang.CharSequence) gr.get("customerId"));
        // campos opcionales en el schema o pueden venir como Utf8
        e.setCardPANMasked((java.lang.CharSequence) gr.get("cardPANMasked"));
        e.setReasonCode((java.lang.CharSequence) gr.get("reasonCode"));
        e.setPriority((java.lang.CharSequence) gr.get("priority"));
        e.setBranchCode((java.lang.CharSequence) gr.get("branchCode"));
        e.setDeliveryAddress((java.lang.CharSequence) gr.get("deliveryAddress"));

        Object ts = gr.get("requestedAt");
        if (ts instanceof Long l) e.setRequestedAt(Instant.ofEpochMilli(l)); // 👈 mapeo clave
        Object at = gr.get("attemptNumber");

        if (at instanceof Integer i) e.setAttemptNumber(i);

        e.setCorrelationId((java.lang.CharSequence) gr.get("correlationId"));
        e.setStatus((java.lang.CharSequence) gr.get("status"));

        return e;
    }



    // Modelo -> Entidad Mongo

    default CardReplacementEntity toEntity(CardReplacementEvent ev) {

        if (ev == null) return null;

        CardReplacementEntity en = new CardReplacementEntity();
        // Avro getters return CharSequence for strings; convert to String safely
        en.setRequestId(ev.getRequestId() != null ? ev.getRequestId().toString() : null);
        en.setCustomerId(ev.getCustomerId() != null ? ev.getCustomerId().toString() : null);
        // The Mongo entity doesn't contain cardPANMasked/reasonCode/priority fields -> omit them
        en.setBranchCode(ev.getBranchCode() != null ? ev.getBranchCode().toString() : null);
        en.setDeliveryAddress(ev.getDeliveryAddress() != null ? ev.getDeliveryAddress().toString() : null);
        en.setRequestedAt(ev.getRequestedAt() != null ? ev.getRequestedAt().toEpochMilli() : 0L);
        // Avro generated getter for attemptNumber is primitive int
        en.setAttemptNumber(ev.getAttemptNumber());
        en.setCorrelationId(ev.getCorrelationId() != null ? ev.getCorrelationId().toString() : null);
        en.setStatus(ev.getStatus() != null ? ev.getStatus().toString() : null);
        return en;

    }

}