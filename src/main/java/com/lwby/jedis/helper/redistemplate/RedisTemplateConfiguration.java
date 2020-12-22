package com.lwby.jedis.helper.redistemplate;

import com.lwby.jedis.helper.config.JedisConfig;
import com.lwby.jedis.helper.constant.RedisConstant;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * redisTemplate启动配置类
 * @author 刘一博
 * @version RedisTemplateConfiguration.java, v 0.1 2020年12月18日 10:50 刘一博 Exp $
 */
@Configuration
public class RedisTemplateConfiguration {

    @Bean
    public RedisClusterConfiguration createJedisCluster(@Autowired JedisConfig jedisConfig) {
        RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
        String nodes = jedisConfig.getNodes();
        if (StringUtils.isNotBlank(nodes)) {
            String[] nodeArray = nodes.split(RedisConstant.DEFAULT_SPLIT);
            Set<RedisNode> nodeList = new HashSet<>();
            for (String node : nodeArray) {
                String[] hostAndPort = node.split(RedisConstant.HOST_AND_PORT_SPLIT);
                nodeList.add(new RedisNode(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            }
            redisClusterConfiguration.setClusterNodes(nodeList);
        }
        return redisClusterConfiguration;
    }

    @Bean
    public JedisPoolConfig createJedisPoolConfig(@Autowired JedisConfig jedisConfig) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(jedisConfig.getMaxIdle());
        poolConfig.setMinIdle(jedisConfig.getMinIdle());
        poolConfig.setTestOnCreate(true);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        return poolConfig;
    }

    @Bean
    public RedisConnectionFactory createJedisConnectionFactory(RedisClusterConfiguration redisClusterConfiguration,
                                                               JedisPoolConfig jedisPoolConfig) {
        return new JedisConnectionFactory(redisClusterConfiguration, jedisPoolConfig);
    }

}
