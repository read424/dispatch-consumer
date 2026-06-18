package com.bank.dispatch_consumer.domain.service;

import com.bank.dispatch_consumer.domain.dto.SnapshotDto;
import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.mapper.EntityMapper;
import com.bank.events.CardReplacementEvent;
import com.bank.dispatch_consumer.domain.repo.CardReplacementRepository;
import com.bank.dispatch_consumer.domain.repo.SnapshotCacheRepository;
import com.bank.dispatch_consumer.infrastructure.kafka.DltProducer;
import com.bank.dispatch_consumer.infrastructure.kafka.EventMessage;
import com.bank.dispatch_consumer.infrastructure.kafka.KafkaRxConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProcessingService {

    private final EntityMapper entityMapper;
    private final CardReplacementRepository cardReplacementRepository;
    private final SnapshotCacheRepository snapshotCacheRepository;
    private final DltProducer dltProducer;
    private final KafkaRxConsumer kafkaRxConsumer;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    private Counter eventsOk;
    private Counter eventsErr;
    private Timer processTimer;

    @PostConstruct
    public void start(){
        // Inicializar metrics
        this.eventsOk = meterRegistry.counter("events_consumed_ok");
        this.eventsErr = meterRegistry.counter("events_consumed_err");
        this.processTimer = meterRegistry.timer("process_time");

        log.info("Iniciando ProcessingService - Suscripción al flujo de Kafka");
        kafkaRxConsumer.flux().subscribe(
                this::processMessage,
                err -> log.error("Error crítico en la suscripción al flujo de Kafka", err)
        );
    }

    private void processMessage(EventMessage<CardReplacementEvent> msg){
        Timer.Sample sample = Timer.start(meterRegistry);

        CardReplacementEvent ev = msg.getPayload();
        if (ev == null) {
            eventsErr.increment();
            log.warn("Payload nulo/inválido recibido, enviando a DLT");
            sendToDlt("unknown", msg);
            try { msg.ack(); } catch (Exception ignored) {}
            sample.stop(processTimer);
            return;
        }

        String requestId = ev.getRequestId() != null ? ev.getRequestId().toString() : ev.getEventId().toString();
        long now = System.currentTimeMillis();

        log.info("Procesando evento - RequestId: {}, EventId: {}, CustomerId: {}, Intento: VERIFICANDO",
                requestId, ev.getEventId(), ev.getCustomerId());

        // Verificar si el documento existe en MongoDB
        cardReplacementRepository.existsByRequestId(requestId).subscribe(
                exists -> {
                    if (exists) {
                        log.info("Documento existe para RequestId: {} - SEGUNDO INTENTO", requestId);
                        processSecondAttempt(ev, msg, now, sample);
                    } else {
                        log.info("Documento NO existe para RequestId: {} - PRIMER INTENTO", requestId);
                        processFirstAttempt(ev, msg, now, sample);
                    }
                },
                err -> {
                    eventsErr.increment();
                    log.error("Error verificando existencia de RequestId: {}", requestId, err);
                    sendToDlt(requestId, msg);
                    try { msg.ack(); } catch (Exception ignored) {}
                    sample.stop(processTimer);
                }
        );
    }

    /**
     * Primer intento: documento no existe
     * 1. Persiste en MongoDB con status = DISPATCHED y attemptNumber = 1
     * 2. Guarda snapshot en Redis con clave card:event:{requestId}
     */
    private void processFirstAttempt(CardReplacementEvent ev, EventMessage<CardReplacementEvent> msg, long receivedAt, Timer.Sample sample) {
        String requestId = ev.getRequestId() != null ? ev.getRequestId().toString() : ev.getEventId().toString();

        CardReplacementEntity entity = entityMapper.toEntity(ev);
        entity.setReceivedAt(receivedAt);
        entity.setProcessedAt(System.currentTimeMillis());
        entity.setStatus("DISPATCHED");
        entity.setAttemptNumber(1);

        try {
            String raw = objectMapper.writeValueAsString(msg.getRawKafkaValue());
            entity.setRawPayload(raw);
        } catch (Exception e){
            entity.setRawPayload(null);
            log.debug("No se pudo serializar el payload raw para RequestId: {}", requestId);
        }

        log.debug("PRIMER INTENTO - Persistiendo en MongoDB para RequestId: {}", requestId);

        // Guardar evento en MongoDB PRIMERO
        cardReplacementRepository.save(entity).subscribe(
                saved -> {
                    log.info("✓ Documento guardado en MongoDB para RequestId: {}", requestId);

                    // LUEGO guardar snapshot en Redis
                    persistSnapshotToRedis(ev, requestId, "DISPATCHED", 1, msg, sample);
                },
                err -> {
                    eventsErr.increment();
                    log.error("✗ Error persistiendo en MongoDB (Primer intento) - RequestId: {}", requestId, err);
                    sendToDlt(requestId, msg);
                    try { msg.ack(); } catch (Exception ignored) {}
                    sample.stop(processTimer);
                }
        );
    }

    /**
     * Segundo intento: documento existe
     * 1. Busca snapshot en Redis
     * 2. Si existe, enriquece evento
     * 3. Actualiza en MongoDB con status = DISPATCHED_CACHE
     * 4. Actualiza snapshot en Redis
     */
    private void processSecondAttempt(CardReplacementEvent ev, EventMessage<CardReplacementEvent> msg, long receivedAt, Timer.Sample sample) {
        String requestId = ev.getRequestId() != null ? ev.getRequestId().toString() : ev.getEventId().toString();

        log.debug("SEGUNDO INTENTO - Buscando snapshot en Redis para RequestId: {}", requestId);

        // Buscar snapshot en Redis para enriquecimiento
        snapshotCacheRepository.getSnapshotJson(requestId).subscribe(
                snapshotJson -> {
                    log.info("✓ Snapshot encontrado en Redis para RequestId: {} - Enriqueciendo evento", requestId);
                    applySnapshot(ev, snapshotJson);
                    persistSecondAttemptWithSnapshot(ev, msg, receivedAt, sample, requestId, true);
                },
                err -> {
                    log.warn("Error leyendo snapshot de Redis (RequestId: {}): {} - Continuando sin enriquecimiento", requestId, err.toString());
                    persistSecondAttemptWithSnapshot(ev, msg, receivedAt, sample, requestId, false);
                },
                () -> {
                    log.debug("Sin snapshot en Redis para RequestId: {} - Continuando sin enriquecimiento", requestId);
                    persistSecondAttemptWithSnapshot(ev, msg, receivedAt, sample, requestId, false);
                }
        );
    }

    private void persistSecondAttemptWithSnapshot(CardReplacementEvent ev, EventMessage<CardReplacementEvent> msg, long receivedAt, Timer.Sample sample, String requestId, boolean hadSnapshot) {
        log.debug("SEGUNDO INTENTO - Actualizando en MongoDB para RequestId: {}", requestId);

        // Obtener documento existente
        cardReplacementRepository.findByRequestId(requestId).subscribe(
                existingEntity -> {
                    // Actualizar con nuevos datos del evento
                    CardReplacementEntity updatedEntity = entityMapper.toEntity(ev);
                    updatedEntity.setRequestId(existingEntity.getRequestId());
                    updatedEntity.setReceivedAt(receivedAt);
                    updatedEntity.setProcessedAt(System.currentTimeMillis());
                    updatedEntity.setStatus("DISPATCHED_CACHE");
                    updatedEntity.setAttemptNumber(2);
                    updatedEntity.setRawPayload(existingEntity.getRawPayload());

                    // Actualizar en MongoDB
                    cardReplacementRepository.save(updatedEntity).subscribe(
                            saved -> {
                                log.info("✓ Documento actualizado en MongoDB para RequestId: {}", requestId);
                                // LUEGO actualizar snapshot en Redis
                                persistSnapshotToRedis(ev, requestId, "DISPATCHED_CACHE", 2, msg, sample);
                            },
                            err -> {
                                eventsErr.increment();
                                log.error("✗ Error actualizando en MongoDB (Segundo intento) - RequestId: {}", requestId, err);
                                sendToDlt(requestId, msg);
                                try { msg.ack(); } catch (Exception ignored) {}
                                sample.stop(processTimer);
                            }
                    );
                },
                err -> {
                    eventsErr.increment();
                    log.error("✗ Error buscando documento existente - RequestId: {}", requestId, err);
                    sendToDlt(requestId, msg);
                    try { msg.ack(); } catch (Exception ignored) {}
                    sample.stop(processTimer);
                }
        );
    }

    /**
     * Persistir snapshot en Redis de forma síncrona y confirmar ack
     */
    private void persistSnapshotToRedis(CardReplacementEvent ev, String requestId, String status, int attemptNumber, EventMessage<CardReplacementEvent> msg, Timer.Sample sample) {
        try {
            SnapshotDto snapshot = SnapshotDto.fromAvroEvent(ev);
            // Actualizar status y attemptNumber con los valores del intento actual
            snapshot.setStatus(status);
            snapshot.setAttemptNumber(attemptNumber);
            String snapshotJson = objectMapper.writeValueAsString(snapshot);
            snapshotCacheRepository.saveSnapshotJson(requestId, snapshotJson).subscribe(
                    () -> {
                        eventsOk.increment();
                        sample.stop(processTimer);
                        log.info("✓ Evento {} (intento {}) - RequestId: {}, Snapshot registrado en Redis (card:event:{})",
                                status, attemptNumber, requestId, requestId);
                        try { msg.ack(); } catch (Exception ignored) {}
                    },
                    redisErr -> {
                        log.warn("Documento guardado en MongoDB pero fallo al guardar snapshot en Redis (RequestId: {}): {}",
                                requestId, redisErr.toString());
                        eventsOk.increment();
                        sample.stop(processTimer);
                        try { msg.ack(); } catch (Exception ignored) {}
                    }
            );
        } catch (Exception e){
            log.warn("Error serializando evento para snapshot (RequestId: {}): {}", requestId, e.toString());
            eventsOk.increment();
            sample.stop(processTimer);
            try { msg.ack(); } catch (Exception ignored) {}
        }
    }

    private void applySnapshot(CardReplacementEvent ev, String snapshotJson){
        if (snapshotJson == null) return;
        try {
            SnapshotDto snapshot = objectMapper.readValue(snapshotJson, SnapshotDto.class);
            if (snapshot.getEventId() != null) ev.setEventId(snapshot.getEventId());
            if (snapshot.getRequestId() != null) ev.setRequestId(snapshot.getRequestId());
            if (snapshot.getCustomerId() != null) ev.setCustomerId(snapshot.getCustomerId());
            if (snapshot.getCardPANMasked() != null) ev.setCardPANMasked(snapshot.getCardPANMasked());
            if (snapshot.getReasonCode() != null) ev.setReasonCode(snapshot.getReasonCode());
            if (snapshot.getPriority() != null) ev.setPriority(snapshot.getPriority());
            if (snapshot.getBranchCode() != null) ev.setBranchCode(snapshot.getBranchCode());
            if (snapshot.getDeliveryAddress() != null) ev.setDeliveryAddress(snapshot.getDeliveryAddress());
            if (snapshot.getRequestedAt() != null) ev.setRequestedAt(snapshot.getRequestedAt());
            if (snapshot.getAttemptNumber() != null) ev.setAttemptNumber(snapshot.getAttemptNumber());
            if (snapshot.getCorrelationId() != null) ev.setCorrelationId(snapshot.getCorrelationId());
            if (snapshot.getStatus() != null) ev.setStatus(snapshot.getStatus());
            log.debug("Snapshot aplicado correctamente a evento");
        } catch (Exception e){
            log.warn("Fallo al mezclar snapshot en evento (requestId={}): {}", ev.getRequestId(), e.toString());
        }
    }

    private void sendToDlt(String key, EventMessage<CardReplacementEvent> msg){
        String payload;
        try { payload = objectMapper.writeValueAsString(msg.getRawKafkaValue()); }
        catch (Exception e){ payload = Objects.toString(msg.getRawKafkaValue(), "<unserializable>"); }
        try {
            log.warn("Enviando mensaje a DLT para Key: {}", key);
            dltProducer.send(key, payload).subscribe();
        } catch (Exception e){
            log.error("Fallo al enviar mensaje a DLT para Key: {}", key, e);
        }
    }

}
