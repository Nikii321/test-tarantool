package com.tarantooltest.tarantool.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "tarantool.command")
@Getter
@Setter
public class TarantoolCommandConfig {
    private String range;
    private String count;

}
