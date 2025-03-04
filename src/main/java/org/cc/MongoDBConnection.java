package org.cc;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ConnectionPoolSettings;

import java.util.Collections;

public class MongoDBConnection {
    private static final String MONGO_HOST = "localhost";
    private static final int MONGO_PORT = 27017;
    private static final MongoClient mongoClient;

    static {
        ConnectionPoolSettings connectionPoolSettings = ConnectionPoolSettings.builder()
                .maxSize(128) // 设置连接池的最大连接数
                .build();

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToConnectionPoolSettings(builder -> builder.applySettings(connectionPoolSettings))
                .applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(new ServerAddress(MONGO_HOST, MONGO_PORT))))
                .build();

        mongoClient = MongoClients.create(settings);
    }

    public static MongoClient getMongoClient() {
        return mongoClient;
    }

    public static void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
