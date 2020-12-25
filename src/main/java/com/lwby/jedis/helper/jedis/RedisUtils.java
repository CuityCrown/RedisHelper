package com.lwby.jedis.helper.jedis;

import com.lwby.jedis.helper.config.JedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisPool;

import javax.annotation.PostConstruct;

/**
 * redis工具类
 *
 * @author 刘一博
 * @version RedisUtils.java, v 0.1 2020年12月25日 13:03 刘一博 Exp $
 */
@Component
public class RedisUtils {

    private final static Logger LOGGER = LoggerFactory.getLogger(RedisUtils.class);

    @Autowired
    private JedisConfig jedisConfig;

    /**
     * 初始化redis工具类
     */
    @PostConstruct
    private void init() {
        try {
            JedisPool jedisPool = new JedisPool();
        } catch (Exception e) {
            LOGGER.error("init RedisUtils is failed", e);
        }

    }

}
