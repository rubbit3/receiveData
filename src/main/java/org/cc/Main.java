package org.cc;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @author wxy
 * @description 数据的接收，开放端口，接收数据，存入数据库and写入文件and存入缓存redis
 * 另外线程执行自动计算和自动推送
 */
public class Main {

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    static final JedisPool jedisPool;
    private static final int REDIS_DB_INDEX = 4;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(256);
        poolConfig.setMaxWait(Duration.ofDays(1));
        poolConfig.setMaxIdle(1024);
        poolConfig.setMinIdle(256);
        jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);
    }


    public static void main(String[] args) throws Exception {
        // 记录服务器启动日志
        logToFile("服务器启动");

        // 注册 JVM 关闭钩子，确保程序意外终止时记录日志
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logToFile("服务器意外终止，可能是手动关闭、系统重启、进程被杀死等原因");
            } catch (Exception e) {
                logToFile("服务器终止时发生错误: " + e.getMessage());
            }
        }));





        // 启动服务器线程
        try {
            OtherRecvServer otherRecvServer = new OtherRecvServer(9999);
            Thread thread = new Thread(otherRecvServer);
            System.out.println("开启多线程接收数据");
            thread.start();
        } catch (Exception e) {
            logToFile("服务器运行异常终止，错误详情: " + e.getMessage());
            throw e; // 让异常继续抛出，避免静默失败
        }


    }


    // **日志写入方法**
    private static void logToFile(String message) {
        String logFilePath = "server.log"; // 当前目录下的日志文件
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(logFilePath, true)))) {
            String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            out.println(timestamp + " - " + message);
        } catch (Exception e) {
            System.err.println("写入日志失败: " + e.getMessage());
        }
    }


}