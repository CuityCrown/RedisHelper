package com.lwby.jedis.helper;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * description:
 *
 * @author 刘一博
 * @version V1.0
 * @date 2020/5/15 14:46
 */
@Configuration
public class JedisConfiguration {

    @Bean
    @ConfigurationProperties(value = "jedis.redis")
    public JedisManager jedisManager() {
        return new JedisManager();
    }

}
