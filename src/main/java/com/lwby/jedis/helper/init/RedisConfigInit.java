package com.lwby.jedis.helper.init;

import com.lwby.jedis.helper.config.JedisConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * spring启动配置类 jedis相关
 *
 * @author 刘一博
 * @version JedisConfiguration.java, v 0.1 2020年5月15日 10:51 刘一博 Exp $
 */
@Configuration
public class RedisConfigInit {

    @Bean
    @ConfigurationProperties(value = "jedis.redis")
    public JedisConfig createJedisConfig() {
        return new JedisConfig();
    }

}
