package com.bank.dispatch_consumer.infrastructure.kafka;

import com.bank.dispatch_consumer.config.KafkaTopicsProperties;
import com.bank.dispatch_consumer.domain.mapper.EntityMapper;
import com.bank.events.CardReplacementEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.core.publisher.Flux;
import java.util.Collections;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaRxConsumer {

    private final ReceiverOptions<String, Object> receiverOptions;

    private final KafkaTopicsProperties topics;

    private final EntityMapper mapper;

    private EventMessage<CardReplacementEvent> mapRecord(ReceiverRecord<String, Object> rec){

        String key = rec.key();
        Object value = rec.value();
        CardReplacementEvent ev = null;

        log.debug("Mensaje recibido del broker - Topic: {}, Partition: {}, Offset: {}, Key: {}",
                rec.receiverOffset().topicPartition().topic(),
                rec.receiverOffset().topicPartition().partition(),
                rec.receiverOffset().offset(),
                key);

        try {
            if (value instanceof GenericRecord gr) {
                ev = mapper.toEvent(gr); // Avro -> dominio
                if (ev != null) {
                    log.info("Evento mapeado correctamente - EventId: {}, RequestId: {}, CustomerId: {}, Timestamp: {}",
                            ev.getEventId(), ev.getRequestId(), ev.getCustomerId(), ev.getRequestedAt());
                }
            } else {
                log.warn("Valor no Avro ({}), se ignora. Key: {}",
                        value == null ? "null" : value.getClass().getSimpleName(), key);
            }
        } catch (Exception e) {
            log.error("Error mapeando Avro -> Event para Key: {}", key, e);
        }
        return new EventMessage<>(ev, rec.receiverOffset(), value);
    }

    // Expose a Flux of mapped EventMessage objects for consumers to subscribe to
    public Flux<EventMessage<CardReplacementEvent>> flux(){
        ReceiverOptions<String,Object> opts = receiverOptions.subscription(Collections.singleton(topics.getMain()));
        return KafkaReceiver.create(opts)
                .receive()
                .map(this::mapRecord)
                .filter(em -> em.getPayload() != null); // filter out unmapped/invalid records
    }
}