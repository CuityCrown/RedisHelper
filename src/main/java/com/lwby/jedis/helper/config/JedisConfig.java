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

    /**
     * 使用数据库编号
     */
    private int database = 0;

    /**
     * ip地址
     */
    private String host = "localhost";

    /**
     * 密码
     */
    private String password;

    /**
     * 端口号
     */
    private int port = 6379;

    /**
     * 超时时间
     */
    private int timeout = 1000;

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

    /**
     * 非活跃连接活跃时间
     */
    private int maxWait = -1;

    /**
     * 集群节点信息
     */
    private String nodes;

    /**
     * 最大重试次数
     */
    private Integer maxRedirects = 1;

}
