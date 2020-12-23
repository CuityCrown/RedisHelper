package com.lwby.jedis.helper.jedis;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.lwby.jedis.helper.config.JedisConfig;
import com.lwby.jedis.helper.constant.RedisConstant;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

/**
 * redis工具类
 *
 * @author 刘一博
 * @version ContentLevelBookFilterService.java, v 0.1 2020年5月15日 10:51 刘一博 Exp $
 */
@Component
public class RedisUtils {

    private final static Logger LOGGER = LoggerFactory.getLogger(RedisUtils.class);

    private JedisCluster jedisCluster;

    @Autowired
    private JedisConfig jedisConfig;

    /**
     * 负无穷
     */
    private static String Z_MIN_SCORE = "-inf";

    /**
     * 正无穷
     */
    private static String Z_MAX_SCORE = "+inf";

    /**
     * 初始化redis工具类
     */
    @PostConstruct
    private void init() {
        try {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            String[] nodes = jedisConfig.getNodes().split(RedisConstant.DEFAULT_SPLIT);
            Set<HostAndPort> hostAndPorts = Arrays.stream(nodes).map(n -> {
                String[] split = n.split(RedisConstant.HOST_AND_PORT_SPLIT);
                return new HostAndPort(split[0], Integer.valueOf(split[1]));
            }).collect(Collectors.toSet());
            jedisCluster = new JedisCluster(hostAndPorts, jedisConfig.getTimeout(), jedisConfig.getTimeout(), jedisConfig.getMaxRedirects(),
                    jedisConfig.getPassword(), jedisPoolConfig);
        } catch (Exception e) {
            LOGGER.error("init RedisUtils is failed", e);
        }

    }

    public <T> T get(String key, TypeReference<T> type) {
        String json = jedisCluster.get(key);
        return JSONObject.parseObject(json, type);
    }

    public void set(String key, Object value) {
        String json = JSONObject.toJSONString(value);
        jedisCluster.set(key, json);
    }

    public void setex(String key, int expires, Object value) {
        String json = JSONObject.toJSONString(value);
        jedisCluster.setex(key, expires, json);
    }

    public <T> T getSet(String key, Object value, TypeReference<T> type) {
        String json = JSONObject.toJSONString(value);
        String oldJson = jedisCluster.getSet(key, json);
        return JSONObject.parseObject(oldJson, type);
    }

    public Boolean exists(String key) {
        return jedisCluster.exists(key);
    }

    public Long incr(String key) {
        return jedisCluster.incr(key);
    }

    public Long incrby(String key, int increment) {
        return jedisCluster.incrBy(key, increment);
    }

    public Long decr(String key) {
        return jedisCluster.decr(key);
    }

    public Long decrBy(String key, int increment) {
        return jedisCluster.decrBy(key, increment);
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

    public Long lrem(String key, long count, Object value) {
        String json = JSONObject.toJSONString(value);
        return jedisCluster.lrem(key, count, json);
    }

    public String ltrim(String key, long start, long end) {
        return jedisCluster.ltrim(key, start, end);
    }

    public String lset(String key, long index, Object value) {
        String json = JSONObject.toJSONString(value);
        return jedisCluster.lset(key, index, json);
    }

    public <T> T lindex(String key, int index, TypeReference<T> type) {
        String json = jedisCluster.lindex(key, index);
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

    public Long hset(String key, String field, Object value) {
        String json = JSONObject.toJSONString(value);
        return jedisCluster.hset(key, field, json);
    }

    public <T> Map<String, T> hgetAll(String key, TypeReference<T> type) {
        Map<String, String> stringMap = jedisCluster.hgetAll(key);
        return stringMap.keySet().stream().collect(Collectors.toMap(k -> k, v -> JSONObject.parseObject(stringMap.get(v), type)));
    }

    public Long hdel(String key, String... fields) {
        return jedisCluster.hdel(key, fields);
    }

    public void zadd(String key, double score, String member) {
        String json = JSONObject.toJSONString(member);
        jedisCluster.zadd(key, score, json);
    }

    public void zadd(String key, double score, Object member) {
        String json = JSONObject.toJSONString(member);
        jedisCluster.zadd(key, score, json);
    }

    public Long zcard(String key) {
        return jedisCluster.zcard(key);
    }

    public Long zcount(String key) {
        return jedisCluster.zcount(key, Z_MIN_SCORE, Z_MAX_SCORE);
    }

    public Long zcount(String key, String min, String max) {
        return jedisCluster.zcount(key, min, max);
    }

    public Long zrem(String key, Object value) {
        String json = JSONObject.toJSONString(value);
        return jedisCluster.zrem(key, json);
    }

    public Long zremrangebyrank(String key, long start, long end) {
        return jedisCluster.zremrangeByRank(key, start, end);
    }

    public Double zscore(String key, String member) {
        return jedisCluster.zscore(key, member);
    }

    public Double zincrby(String key, double score, Object value) {
        String json = JSONObject.toJSONString(value);
        return jedisCluster.zincrby(key, score, json);
    }

    public <T> Map<T, Double> zrange(String key, long start, long end, TypeReference<T> type) {
        Set<Tuple> tuples = jedisCluster.zrangeWithScores(key, start, end);
        return tuples.stream().collect(Collectors.toMap(k -> JSONObject.parseObject(k.getElement(), type), Tuple::getScore));
    }

    /**
     * 批量get
     *
     * @param list 需要获取的Key列表
     * @param type 获取数据的类型
     * @param <T>  泛型
     * @return 结果列表
     */
    public <T> List<T> mget(List<String> list, TypeReference<T> type) {
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        String[] keyArray = list.toArray(new String[0]);
        List<String> jsonList = jedisCluster.mget(keyArray);
        if (CollectionUtils.isNotEmpty(jsonList)) {
            List<T> result = new ArrayList<>();
            for (String json : jsonList) {
                result.add(JSONObject.parseObject(json, type));
            }
            return result;
        }
        return null;
    }

    /**
     * 批量set
     *
     * @param map 需要执行set的key 和 value
     */
    public void mset(Map<String, Object> map) {
        if (map == null || map.size() <= 0) {
            return;
        }
        List<String> list = new ArrayList<>();
        for (String key : map.keySet()) {
            list.add(key);
            list.add(JSONObject.toJSONString(map.get(key)));
        }
        String[] keysValues = list.toArray(new String[] {});
        jedisCluster.mset(keysValues);
    }

}
