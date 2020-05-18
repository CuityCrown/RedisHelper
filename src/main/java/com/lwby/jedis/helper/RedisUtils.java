package com.lwby.jedis.helper;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * description:
 *
 * @author 刘一博
 * @version V1.0
 * @date 2020/5/15 10:51
 */
@Component
public class RedisUtils {

    private final static Logger logger = LoggerFactory.getLogger(RedisUtils.class);

    private JedisCluster jedisCluster;

    @Autowired
    private JedisManager jedisManager;

    @PostConstruct
    private void init() {
        try {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            String[] nodes = jedisManager.getNodes().split(",");
            Set<HostAndPort> hostAndPorts = Arrays.stream(nodes).map(n -> {
                String[] split = n.split(":");
                return new HostAndPort(split[0], Integer.valueOf(split[1]));
            }).collect(Collectors.toSet());
            jedisCluster = new JedisCluster(hostAndPorts, jedisManager.getTimeout(), jedisManager.getTimeout(), jedisManager.getMaxRedirects(), jedisManager.getPassword(), jedisPoolConfig);
        } catch (Exception e) {
            logger.error("init RedisUtils is failed", e);
        }

    }

    public <T> T get(String key, TypeReference<T> type) {
        String s = jedisCluster.get(key);
        T t = JSONObject.parseObject(s, type);
        return t;
    }

    public void set(String key, Object value) {
        String json = JSONObject.toJSONString(value);
        jedisCluster.set(key, json);
    }

    public void setex(String key, int expires, Object value) {
        String json = JSONObject.toJSONString(value);
        jedisCluster.setex(key, expires, json);
    }

    public Long del(String key) {
        return jedisCluster.del(key);
    }

    public Long expire(String key, int expires) {
        return jedisCluster.expire(key, expires);
    }

    public Long ttl(String key) {
        return jedisCluster.ttl(key);
    }

    public Long llen(String key) {
        return jedisCluster.llen(key);
    }

    public <T> List<T> lrange(String key, long start, long end, TypeReference<T> type) {
        List<String> list = jedisCluster.lrange(key, start, end);
        return list.stream()
                .map(json -> JSONObject.parseObject(json, type))
                .collect(Collectors.toList());
    }

    public Long lpush(String key, Object value) {
        String json = JSONObject.toJSONString(value);
        return jedisCluster.lpush(key, json);
    }

    public <T> T lpop(String key, TypeReference<T> type) {
        String json = jedisCluster.lpop(key);
        return JSONObject.parseObject(json, type);
    }

    public Long rpush(String key, Object value) {
        String json = JSONObject.toJSONString(value);
        return jedisCluster.rpush(key, json);
    }

    public <T> T rpop(String key, TypeReference<T> type) {
        String json = jedisCluster.rpop(key);
        return JSONObject.parseObject(json, type);
    }

    public <T> Map<T, Double> zrevrangeWithScores(String key, long start, long end, TypeReference<T> type) {
        Set<Tuple> tuples = jedisCluster.zrevrangeWithScores(key, start, end);
        return tuples.stream().collect(Collectors.toMap(k -> JSONObject.parseObject(k.getElement(), type), Tuple::getScore));
    }

    public <T> T hget(String key, String field, TypeReference<T> type) {
        String json = jedisCluster.hget(key, field);
        return JSONObject.parseObject(json, type);
    }

    public void zadd(String key, double score, String member) {
        String json = JSONObject.toJSONString(member);
        jedisCluster.zadd(key, score, json);
    }

}
