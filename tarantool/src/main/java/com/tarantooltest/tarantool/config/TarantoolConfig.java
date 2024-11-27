package com.tarantooltest.tarantool.config;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TarantoolConfig {
    private final String host;
    private final int port;

    @Autowired
    public TarantoolConfig(
            @Value("${tarantool.host}") String host,
            @Value("${tarantool.port}") int port
    ) {
        this.host = host;
        this.port = port;
    }

    @Bean
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> init() {
        return TarantoolClientFactory
                .createClient()
                .withAddress(host, port)
                .withConnectTimeout(1000 * 60)
                .build();
    }
}


