package com.tarantooltest.tarantool.service;

import com.tarantool.grpc.Scheme;
import com.tarantool.grpc.Scheme.IsDeleted;
import com.tarantool.grpc.Scheme.KeyValueModel;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public interface TarantoolService {
    CompletableFuture<IsDeleted> delete(String value);

    CompletableFuture<KeyValueModel> put(KeyValueModel entity);

    CompletableFuture<Scheme.CountResponse> count();

    CompletableFuture<Stream<KeyValueModel>> range(String keySince, String keyTo);

    CompletableFuture<KeyValueModel> get(String key);
}
