package com.bank.dispatch_consumer.domain.repo;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

public interface CardReplacementRepository {

    Maybe<CardReplacementEntity> findByRequestId(String requestId);

    Single<Boolean> existsByRequestId(String requestId);

    Single<CardReplacementEntity> save(CardReplacementEntity entity);

}