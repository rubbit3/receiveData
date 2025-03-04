package org.cc;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.Arrays;

public class MongoDBUtil {
    // MongoDB连接字符串
    private static final String MONGODB_URI = "mongodb://localhost:27017/?retryWrites=false";
    // MongoDB数据库名称
    private static final String DATABASE_NAME = "zjzsdata";
    // 重连阈值，单位毫秒
    private static final long RECONNECT_THRESHOLD = 5000;

    private static MongoClient mongoClient;
    private static long lastConnectionTime = 0;

    // 获取MongoDB数据库连接
    public static synchronized MongoDatabase getDatabase() {
        if (mongoClient == null || isReconnectNeeded()) {
            int retryCount = 3;
            reconnect(retryCount);
        }
        lastConnectionTime = System.currentTimeMillis();
        return mongoClient.getDatabase(DATABASE_NAME);
    }

    // 连接或重新连接MongoDB
    private static void reconnect(int retryCount) {
        close();
        for (int i = 0; i < retryCount; i++) {
            try {
                mongoClient = MongoClients.create(MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(MONGODB_URI))
                        .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress("localhost", 27017))))
                        .build());
                if (mongoClient != null) {
                    return;
                }
            } catch (Exception e) {
                System.err.println("Failed to connect to MongoDB: " + e.getMessage());
            }
            try {
//                System.err.println("Retrying connection to MongoDB (" + (i + 1) + "/" + retryCount + ")");
                Thread.sleep(2000); // 等待2秒再重试
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted during retry wait: " + ie.getMessage());
                break;
            }
        }
        System.err.println("Failed to connect to MongoDB after " + retryCount + " attempts.");
    }

    // 检查是否需要重新连接
    private static boolean isReconnectNeeded() {
        return (System.currentTimeMillis() - lastConnectionTime) > RECONNECT_THRESHOLD;
    }

    // 关闭MongoDB连接
    public static synchronized void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }
}
