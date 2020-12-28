package com.lwby.jedis.helper.jedis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.lwby.jedis.helper.config.JedisConfig;
import com.lwby.jedis.helper.constant.RedisConstant;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

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

    private JedisPool jedisPool;

    /**
     * 初始化redis工具类
     */
    @PostConstruct
    private void init() {
        try {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(jedisConfig.getMaxTotal());
            jedisPoolConfig.setMinIdle(jedisConfig.getMinIdle());
            jedisPoolConfig.setMaxIdle(jedisConfig.getMaxIdle());
            jedisPoolConfig.setMaxWaitMillis(jedisConfig.getMaxWait());
            jedisPool = new JedisPool(jedisPoolConfig, jedisConfig.getHost(), jedisConfig.getPort(), jedisConfig.getTimeout());
        } catch (Exception e) {
            LOGGER.error("init RedisUtils is failed", e);
        }
    }

    public <T> T get(String key, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(key);
            return JSONObject.parseObject(json, type);
        }
    }

    public void set(String key, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            jedis.set(key, json);
        }
    }

    public void setex(String key, int expires, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            jedis.setex(key, expires, json);
        }
    }

    public <T> T getSet(String key, Object value, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            String oldJson = jedis.getSet(key, json);
            return JSONObject.parseObject(oldJson, type);
        }
    }

    public Boolean exists(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(key);
        }
    }

    public Long incr(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.incr(key);
        }
    }

    public Long incrby(String key, int increment) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.incrBy(key, increment);
        }
    }

    public Long decr(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.decr(key);
        }
    }

    public Long decrBy(String key, int increment) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.decrBy(key, increment);
        }
    }

    public Long del(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.del(key);
        }
    }

    public Long expire(String key, int expires) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.expire(key, expires);
        }
    }

    public Long ttl(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.ttl(key);
        }
    }

    public Long llen(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.llen(key);
        }
    }

    public <T> List<T> lrange(String key, long start, long end, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> list = jedis.lrange(key, start, end);
            return list.stream()
                    .map(json -> JSONObject.parseObject(json, type))
                    .collect(Collectors.toList());
        }
    }

    public Long lpush(String key, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            return jedis.lpush(key, json);
        }
    }

    public <T> T lpop(String key, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.lpop(key);
            return JSONObject.parseObject(json, type);
        }
    }

    public Long rpush(String key, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            return jedis.rpush(key, json);
        }
    }

    public <T> T rpop(String key, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.rpop(key);
            return JSONObject.parseObject(json, type);
        }
    }

    public Long lrem(String key, long count, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            return jedis.lrem(key, count, json);
        }
    }

    public String ltrim(String key, long start, long end) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.ltrim(key, start, end);
        }
    }

    public String lset(String key, long index, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            return jedis.lset(key, index, json);
        }
    }

    public <T> T lindex(String key, int index, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.lindex(key, index);
            return JSONObject.parseObject(json, type);
        }
    }

    public <T> Map<T, Double> zrevrangeWithScores(String key, long start, long end, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<Tuple> tuples = jedis.zrevrangeWithScores(key, start, end);
            return tuples.stream().collect(Collectors.toMap(k -> JSONObject.parseObject(k.getElement(), type), Tuple::getScore));
        }
    }

    public <T> T hget(String key, String field, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.hget(key, field);
            return JSONObject.parseObject(json, type);
        }
    }

    public Long hset(String key, String field, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            return jedis.hset(key, field, json);
        }
    }

    public <T> Map<String, T> hgetAll(String key, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> stringMap = jedis.hgetAll(key);
            return stringMap.keySet().stream().collect(Collectors.toMap(k -> k, v -> JSONObject.parseObject(stringMap.get(v), type)));
        }
    }

    public Long hdel(String key, String... fields) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hdel(key, fields);
        }
    }

    public void zadd(String key, double score, String member) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(member);
            jedis.zadd(key, score, json);
        }
    }

    public void zadd(String key, double score, Object member) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(member);
            jedis.zadd(key, score, json);
        }
    }

    public Long zcard(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zcard(key);
        }
    }

    public Long zcount(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zcount(key, RedisConstant.Z_MIN_SCORE, RedisConstant.Z_MAX_SCORE);
        }
    }

    public Long zcount(String key, String min, String max) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zcount(key, min, max);
        }
    }

    public Long zrem(String key, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            return jedis.zrem(key, json);
        }
    }

    public Long zremrangebyrank(String key, long start, long end) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zremrangeByRank(key, start, end);
        }
    }

    public Double zscore(String key, String member) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zscore(key, member);
        }
    }

    public Double zincrby(String key, double score, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(value);
            return jedis.zincrby(key, score, json);
        }
    }

    public <T> Map<T, Double> zrange(String key, long start, long end, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<Tuple> tuples = jedis.zrangeWithScores(key, start, end);
            return tuples.stream().collect(Collectors.toMap(k -> JSONObject.parseObject(k.getElement(), type), Tuple::getScore));
        }
    }

    public <T> Map<T, Double> zrevrange(String key, long start, long end, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<Tuple> tuples = jedis.zrevrangeWithScores(key, start, end);
            return tuples.stream().collect(Collectors.toMap(k -> JSONObject.parseObject(k.getElement(), type), Tuple::getScore));
        }
    }

    /**
     * 批量get 注意: 1.需要保证批量执行的key在同一个hash槽 2.控制数据倾斜程度
     *
     * @param list 需要获取的Key列表
     * @param type 获取数据的类型
     * @param <T>  泛型
     * @return 结果列表
     */
    public <T> List<T> mget(List<String> list, TypeReference<T> type) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (CollectionUtils.isEmpty(list)) {
                return null;
            }
            String[] keyArray = list.toArray(new String[0]);
            List<String> jsonList = jedis.mget(keyArray);
            if (CollectionUtils.isNotEmpty(jsonList)) {
                List<T> result = new ArrayList<>();
                for (String json : jsonList) {
                    if (StringUtils.isNotBlank(json)) {
                        result.add(JSONObject.parseObject(json, type));
                    }
                }
                return result;
            }
            return null;
        }
    }

    /**
     * 批量set 注意: 1.需要保证批量执行的key在同一个hash槽 2.控制数据倾斜程度
     *
     * @param map 需要执行set的key 和 value
     */
    public void mset(Map<String, Object> map) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (map == null || map.size() <= 0) {
                return;
            }
            List<String> list = new ArrayList<>();
            for (String key : map.keySet()) {
                list.add(key);
                list.add(JSON.toJSONString(map.get(key)));
            }
            String[] keysValues = list.toArray(new String[] {});
            jedis.mset(keysValues);
        }
    }

    /**
     * 根据key获取数据列表 如果缓存不存在回调callable并存入缓存
     *
     * @param key      redisKey
     * @param type     数据类型
     * @param callable 缓存不存在回调接口
     * @param <T>      数据泛型
     * @return 数据列表
     */
    public <T> List<T> getList(String key, TypeReference<List<T>> type, int expires, Callable<List<T>> callable) {
        try (Jedis jedis = jedisPool.getResource()) {
            List<T> result = null;
            try {
                String json = jedis.get(key);
                if (StringUtils.isNotBlank(json)) {
                    result = JSONObject.parseObject(json, type);
                } else {
                    result = callable.call();
                    if (CollectionUtils.isNotEmpty(result)) {
                        jedis.setex(key, expires, JSON.toJSONString(result));
                    }
                }
            } catch (Exception e) {
                LOGGER.error("RedisUtils.getList 执行出错 redisKey:{}", key, e);
            }
            return result;
        }
    }

    /**
     * 根据key获取数据 如果缓存不存在回调callable并存入缓存
     *
     * @param key      redisKey
     * @param type     数据类型
     * @param callable 缓存不存在回调接口
     * @param <T>      数据泛型
     * @return 数据列表
     */
    public <T> T get(String key, TypeReference<T> type, int expires, Callable<T> callable) {
        try (Jedis jedis = jedisPool.getResource()) {
            T result = null;
            try {
                String json = jedis.get(key);
                if (StringUtils.isNotBlank(json)) {
                    result = JSONObject.parseObject(json, type);
                } else {
                    result = callable.call();
                    if (result != null) {
                        jedis.setex(key, expires, JSON.toJSONString(result));
                    }
                }
            } catch (Exception e) {
                LOGGER.error("RedisUtils.get 执行出错 redisKey:{}", key, e);
            }
            return result;
        }
    }

}
