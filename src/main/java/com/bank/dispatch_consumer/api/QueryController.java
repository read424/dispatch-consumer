package com.bank.dispatch_consumer.api;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.repo.CardReplacementRepository;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class QueryController {

    private final CardReplacementRepository repo;

    @GetMapping("/events")
    public Single<ResponseEntity<CardReplacementEntity>> byQuery(@RequestParam String requestId) {
        return repo.findByRequestId(requestId)
                .map(ResponseEntity::ok)
                .switchIfEmpty(Maybe.just(ResponseEntity.notFound().build()))
                .toSingle();
    }


    @GetMapping("/health")
    public ResponseEntity<String> health() { return ResponseEntity.ok("OK"); }
}