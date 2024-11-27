package com.tarantooltest.tarantool.service;

import com.google.protobuf.ByteString;
import com.tarantool.grpc.Scheme.CountResponse;
import com.tarantool.grpc.Scheme.IsDeleted;
import com.tarantool.grpc.Scheme.KeyValueModel;
import com.tarantooltest.tarantool.config.TarantoolCommandConfig;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;


@Service
@Slf4j
public class TarantoolServiceImpl implements TarantoolService {
    private final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private final TarantoolTupleFactory tupleFactory =
            new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());
    private final TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> spaceOperations;
    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;
    private final String index;
    private final TarantoolCommandConfig tarantoolCommandConfig;
    @Autowired
    public TarantoolServiceImpl(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient,
            @Value("${tarantool.space}") String space,
            @Value("${tarantool.index}") String index,
            TarantoolCommandConfig tarantoolCommandConfig) {
        this.tarantoolClient = tarantoolClient;
        this.tarantoolCommandConfig = tarantoolCommandConfig;
        this.spaceOperations = tarantoolClient.space(space);
        this.index = index;
    }

    @Override
    public CompletableFuture<IsDeleted> delete(String id) {
        return spaceOperations
                .delete(Conditions.indexEquals(index, List.of(id)))
                .thenApply((it) -> {
                    if (it.isEmpty()) {
                        log.warn("Delete operation failed, ID not found: {}", id);
                        throw new NoSuchElementException("No entry found for ID: " + id);
                    }
                    log.info("Successfully deleted ID: {}", id);
                    return IsDeleted.newBuilder().setValue(true).build();
                }).exceptionally(ex -> {
                    log.error("Error during delete operation for ID: {}", id, ex);
                    throw new CompletionException(ex);
                });
    }

    @Override
    public CompletableFuture<KeyValueModel> put(KeyValueModel entity) {
        TarantoolTuple tarantoolTuple = tupleFactory.create(Arrays.asList(entity.getKey(), entity.getValue().toByteArray()));
        return spaceOperations.replace(tarantoolTuple).thenApply(res -> {
                    if (res.isEmpty()) {
                        throw new IllegalStateException("Insert operation returned an empty result");
                    }
                    log.info("Successfully inserted entity with key: {}", entity.getKey());
                    return toModel(res.get(0));
                }
        );
    }

    @Override
    public CompletableFuture<CountResponse> count() {
        return tarantoolClient.call(tarantoolCommandConfig.getCount())
                .thenApply(res -> {
                    log.info("Count operation returned size: {}", res.get(0));
                    return CountResponse.newBuilder().setValue((int) res.get(0)).build();
                }).exceptionally(ex -> {
                    log.error("Error during count operation", ex);
                    throw new CompletionException(ex);
                });
    }

    @Override
    public CompletableFuture<Stream<KeyValueModel>> range(String keySince, String keyTo) {
        if (keySince == null || keyTo == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Keys for range cannot be null"));
        }
        return tarantoolClient.call(tarantoolCommandConfig.getRange(), keySince, keyTo).thenApply(res -> {
            ArrayList<TarantoolTuple> result = (ArrayList<TarantoolTuple>) res.get(0);
            log.info("Range operation returned {} results", res.size());
            return result.stream().map(this::toModel);
        }).exceptionally(ex -> {
            log.error("Error during range operation for keys {} to {}", keySince, keyTo, ex);
            throw new CompletionException(ex);
        });
    }

    @Override
    public CompletableFuture<KeyValueModel> get(String key) {
        if (key == null || key.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Key cannot be null or empty"));
        }
        return spaceOperations.select(Conditions.indexEquals(index, List.of(key)))
                .thenApply(res -> {
                    if (res.isEmpty()) {
                        log.warn("Get operation failed, key not found: {}", key);
                        throw new NoSuchElementException("No entry found for key: " + key);
                    }
                    log.info("Successfully retrieved entity with key: {}", key);
                    return toModel(res.get(0));
                }).exceptionally(ex -> {
                    log.error("Error during get operation for key: {}", key, ex);
                    throw new CompletionException(ex);
                });
    }

    private KeyValueModel toModel(TarantoolTuple tuple) {
        return KeyValueModel.newBuilder()
                .setKey(tuple.getString("key")).
                setValue(ByteString.copyFrom(tuple.getByteArray("value"))).build();
    }
}
