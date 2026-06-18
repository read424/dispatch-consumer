package com.bank.dispatch_consumer.infrastructure.redis;

import com.bank.dispatch_consumer.domain.repo.SnapshotCacheRepository;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.adapter.rxjava.RxJava3Adapter;
import java.time.Duration;

@Component
@RequiredArgsConstructor
@Slf4j
public class RedisSnapshotCacheRepository implements SnapshotCacheRepository {

    private final ReactiveStringRedisTemplate redis;
    private static final long DEFAULT_TTL_SECONDS = 86400; // 24 horas


    @Override
    public Maybe<String> getSnapshotJson(String requestId) {
        String key = key(requestId);
        log.debug("Leyendo snapshot de Redis con clave: {}", key);
        return RxJava3Adapter.monoToMaybe(redis.opsForValue().get(key));
    }

    @Override
    public Completable saveSnapshotJson(String requestId, String snapshotJson) {
        String key = key(requestId);
        log.debug("Guardando snapshot en Redis con clave: {}, TTL: {} segundos", key, DEFAULT_TTL_SECONDS);
        return RxJava3Adapter.monoToCompletable(
                redis.opsForValue().set(key, snapshotJson, Duration.ofSeconds(DEFAULT_TTL_SECONDS))
                    .doOnNext(success -> log.info("✓ Snapshot guardado en Redis: {}", key))
                    .doOnError(err -> log.error("✗ Error guardando snapshot en Redis ({}): {}", key, err.toString()))
        );
    }

    private String key(String id){
        return "card:event:" + id;
    }
}