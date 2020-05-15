package com.lwby.jedis.helper;

import lombok.Data;

/**
 * description:
 *
 * @author 刘一博
 * @version V1.0
 * @date 2020/5/15 11:47
 */

@Data
public class JedisManager {

    private int database = 0;

    private String host = "localhost";

    private String password;

    private int port = 6379;

    private int timeout;

    private int maxIdle = 8;

    private int minIdle = 0;

    private int maxActive = 8;

    private int maxWait = -1;

    private String master;

    private String nodes;

    private Integer maxRedirects;

}
