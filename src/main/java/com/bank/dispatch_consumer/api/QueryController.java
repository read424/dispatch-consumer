package com.bank.dispatch_consumer.api;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.repo.CardReplacementRepository;
import com.bank.dispatch_consumer.domain.repo.SnapshotCacheRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class QueryController {

    private final CardReplacementRepository repo;
    private final SnapshotCacheRepository snapshotCache;
    private final ObjectMapper objectMapper;

    @GetMapping("/events")
    public Single<ResponseEntity<CardReplacementEntity>> byRequestId(@RequestParam String requestId) {
        return snapshotCache.getSnapshotJson(requestId)
                .flatMapSingle(snapshotJson -> validateSnapshot(snapshotJson, requestId))
                .onErrorResumeNext(err -> {
                    log.debug("Snapshot de Redis inválido ({}), recurriendo a MongoDB", err.getMessage());
                    return repo.findByRequestId(requestId);
                })
                .switchIfEmpty(Maybe.defer(() -> repo.findByRequestId(requestId)))
                .map(ResponseEntity::ok)
                .switchIfEmpty(Maybe.just(ResponseEntity.notFound().build()))
                .toSingle();
    }

    private Single<CardReplacementEntity> validateSnapshot(String snapshotJson, String requestId) {
        return Single.fromCallable(() -> {
            CardReplacementEntity event = objectMapper.readValue(snapshotJson, CardReplacementEntity.class);

            if (event.getRequestId() == null || event.getRequestId().isEmpty()) {
                throw new IllegalArgumentException("Snapshot corrupto: requestId ausente");
            }

            if (!"DISPATCHED_CACHE".equals(event.getStatus())) {
                log.warn("Snapshot inválido para RequestId {}: status es {} en lugar de DISPATCHED_CACHE",
                        requestId, event.getStatus());
                throw new IllegalArgumentException("Status inválido: " + event.getStatus());
            }

            if (event.getAttemptNumber() != 2) {
                log.warn("Snapshot inválido para RequestId {}: attemptNumber es {} en lugar de 2",
                        requestId, event.getAttemptNumber());
                throw new IllegalArgumentException("AttemptNumber inválido: " + event.getAttemptNumber());
            }

            log.info("✓ Snapshot validado de Redis para RequestId: {}", requestId);
            return event;
        });
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() { return ResponseEntity.ok("OK"); }
}