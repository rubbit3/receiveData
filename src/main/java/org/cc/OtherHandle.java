package org.cc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.utils.StringUtils;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.bson.Document;
import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.GZIPOutputStream;


import static org.cc.Main.jedisPool;
import static org.cc.MongoDBConnection.getMongoClient;


/**
 * todo 处理数据，===接受数据，每轮都是数组，需要拆分出来
 */
public class OtherHandle extends SimpleChannelInboundHandler<String> {

    MongoClient mongoClient = getMongoClient();
    MongoDatabase database1 = mongoClient.getDatabase("zjzsdata");

    private boolean CheckDataCorrect(String content) {
        if (StringUtils.isEmpty(content)) {
            return false;
        }
        boolean isJsonObject = true;
        boolean isJsonArray = true;
        try {
            JSONObject.parseObject(content);
        } catch (Exception e) {
            isJsonObject = false;
        }
        try {
            JSONObject.parseArray(content);
        } catch (Exception e) {
            isJsonArray = false;
        }
        if (!isJsonObject && !isJsonArray) { //不是json格式
            return false;
        }
        return true;
    }

    //    每轮处理数据
    private void HandleData(String content, String rip, Channel ch) {

        try {
            // 解析JSON数据
            JSONArray array = JSONArray.parseArray(content);
            int arraySize = array.size();

            // 初始化数据缓冲队列
            Map<String, List<Document>> bufferMap = new HashMap<>();
            Map<String, List<Document>> bufferMaxMap = new HashMap<>();

            // 数据处理并按ID分组
            for (int i = 0; i < arraySize; i++) {
                JSONObject jsonObject = array.getJSONObject(i);
                String id = jsonObject.getString("id");
                Long startTime = jsonObject.getLong("StartTime") / 1000;
                jsonObject.put("checkTime", startTime);
                jsonObject.put("timestamp", startTime);

                // 转换JSON为Document
                Document doc = Document.parse(jsonObject.toJSONString());
                bufferMap.computeIfAbsent(id, k -> new LinkedList<>()).add(doc);

                // 计算Data字段的统计数据
                JSONArray dataArray = jsonObject.getJSONArray("Data");
                if (dataArray != null && !dataArray.isEmpty()) {
                    double[] dataArrayConverted = dataArray.toJavaList(Double.class).stream().mapToDouble(Double::doubleValue).toArray();
                    double max = Arrays.stream(dataArrayConverted).max().orElse(0.0);
                    double min = Arrays.stream(dataArrayConverted).min().orElse(0.0);
                    double avg = Arrays.stream(dataArrayConverted).average().orElse(0.0);

                    max = Math.round(max * 100000.0) / 100000.0;
                    min = Math.round(min * 100000.0) / 100000.0;
                    avg = Math.round(avg * 100000.0) / 100000.0;

                    Document docMax = new Document("max", max)
                            .append("min", min)
                            .append("avg", avg)
                            .append("ptp", max - min)
                            .append("Unit", jsonObject.get("Unit"))
                            .append("id", id)
                            .append("checkTime", startTime);

                    bufferMaxMap.computeIfAbsent(id, k -> new LinkedList<>()).add(docMax);
                }
            }

            // 处理MongoDB插入逻辑
            String currentDate = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
            File dayDirectory = getFile(currentDate);

            bufferMap.forEach((id, documents) -> {
                MongoCollection<Document> collection = database1.getCollection(id);
                MongoCollection<Document> collectionmax = database1.getCollection(id + "Tmax");

                collection.createIndex(Indexes.ascending("checkTime"));
                collectionmax.createIndex(Indexes.ascending("checkTime"));
                collectionmax.createIndex(Indexes.ascending("timestamp"));

                if (!documents.isEmpty()) {
                    collection.insertMany(documents);
                }

                List<Document> documentsmax = bufferMaxMap.get(id);
                if (documentsmax != null && !documentsmax.isEmpty()) {
                    collectionmax.insertMany(documentsmax);
                }

                // 实时数据写入Redis
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.select(2);
                    documents.forEach(doc -> {
                        jedis.rpush(id, doc.toJson());
                    });
                    jedis.ltrim(id, -60, -1);
                }
            });

            bufferMap.clear();
            bufferMaxMap.clear();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static File getFile(String currentDate) {
        String[] dateComponents = currentDate.split("/");
        String year = dateComponents[0];
        String month = dateComponents[1];
        String day = dateComponents[2];

        // Create directories for year, month, and day
        File yearDirectory = new File(year);
        if (!yearDirectory.exists()) {
            yearDirectory.mkdir();
        }
        File monthDirectory = new File(yearDirectory, month);
        if (!monthDirectory.exists()) {
            monthDirectory.mkdir();
        }
        File dayDirectory = new File(monthDirectory, day);
        if (!dayDirectory.exists()) {
            dayDirectory.mkdir();
        }
        return dayDirectory;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext arg0, String arg1) throws Exception {
        // TODO Auto-generated method stub
        String rip = arg0.channel().remoteAddress().toString().substring(1);

        if (CheckDataCorrect(arg1)) {
            HandleData(arg1, rip, arg0.channel());
        }
    }
}
