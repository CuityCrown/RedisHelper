package com.lwby.jedis.helper.config;

import lombok.Data;

/**
 * jedis配置项封装类
 *
 * @author 刘一博
 * @version JedisConfig.java, v 0.1 2020年5月15日 10:51 刘一博 Exp $
 */
@Data
public class JedisConfig {

    private int database = 0;

    private String host = "localhost";

    private String password;

    private int port = 6379;

    /**
     * 超时时间
     */
    private int timeout;

    /**
     * 最大空闲连接数
     */
    private int maxIdle = 10;

    /**
     * 最小空闲连接数
     */
    private int minIdle = 0;

    /**
     * 最大连接数
     */
    private int maxTotal = 10;

    private int maxWait = -1;

    private String master;

    private String nodes;

    private Integer maxRedirects = 1;

}
