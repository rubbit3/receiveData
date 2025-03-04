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
 * 的
 */
public class oldOhterHandle extends SimpleChannelInboundHandler<String> {

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

            //如果数据为有效的JSON包
            JSONArray array = JSONArray.parseArray(content);
//            数据处理，循环
            int arraySize = array.size();
//            System.out.println("arraySize:" + arraySize);

            // 1、索引出array的每一个元素，元素也是一个JSON对象，需要根据其中一个字段为id区分元素
            Map<String, List<JSONObject>> bufferMap = new HashMap<>();


//            数组 每一轮的数据,区分出不同的消息，相同id消息存储在同一个id消息队列中，存入数据缓冲队列中
            for (int i = 0; i < arraySize; i++) {

                JSONObject jsonObject = array.getJSONObject(i);
                String id = jsonObject.getString("id");

//                if (id.equals("973110#7-1")) {
//                    jsonObject.put("channel_name", "桥梁东侧强震计");
//                    jsonObject.put("dir", "X");
//                }

                Long startTime = jsonObject.getLong("StartTime");
                jsonObject.put("checkTime", startTime / 1000);//接收时间
                // 将毫秒级时间戳转换为 Instant 对象
                Instant instant = Instant.ofEpochMilli(startTime);
                // 将 Instant 转换为特定时区的 ZonedDateTime
                ZonedDateTime dateTime = instant.atZone(ZoneId.systemDefault());
                // 定义日期时间格式
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                // 格式化日期时间
                String formattedDateTime = dateTime.format(formatter);
                // 将格式化的日期时间添加到 jsonObject 中
                jsonObject.put("formattedStartTime", formattedDateTime);
                jsonObject.put("timestamp", startTime / 1000);

                // 判断是否有 "Data" 字段

                JSONArray dataArray = jsonObject.getJSONArray("Data");

                if (dataArray != null && !dataArray.isEmpty()) {
                    // 转换为 Java List<Double>，然后转换为数组
                    List<Double> dataList = dataArray.toJavaList(Double.class);
                    double[] dataArrayConverted = dataList.stream().mapToDouble(Double::doubleValue).toArray();

                    // 初始化变量
                    double min = Double.MAX_VALUE;
                    double max = Double.MIN_VALUE;
                    double sum = 0.0;

                    // 遍历数组，计算最大值、最小值、总和
                    for (double value : dataArrayConverted) {
                        if (value > max) max = value;
                        if (value < min) min = value;
                        sum += value;
                    }

                    // 计算平均值
                    double avg = sum / dataArrayConverted.length;

                    // **使用 Math.round() 方式保留5位小数**
                    max = Math.round(max * 100000.0) / 100000.0;
                    min = Math.round(min * 100000.0) / 100000.0;
                    avg = Math.round(avg * 100000.0) / 100000.0;

                    // 输出结果

                    jsonObject.put("max", max);
                    jsonObject.put("min", min);
                    jsonObject.put("avg", avg);
                    jsonObject.put("ptp", max - min);

                }


                // 创建不同的ID缓冲空队列，向相同id的对象存入同一个ID的缓冲队列中
                bufferMap.computeIfAbsent(id, k -> new LinkedList<>()).add(jsonObject);

            }


            // Get current date components
            String currentDate = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
            File dayDirectory = getFile(currentDate);


            // 3、将不同的缓冲队列的数据写入MongoDB的集合，每一个不同ID写入不同的集合，如果集合没有就新建，如果有就直接写入

//            缓冲队列，循环取出每一个队列的元素
            for (Map.Entry<String, List<JSONObject>> entry : bufferMap.entrySet()) {


                String id = entry.getKey();
                File file = new File(dayDirectory, id + ".json.gz");

                List<JSONObject> jsonObjects = entry.getValue();


                // 写数据到文件中
//                try (GZIPOutputStream gzipOut = new GZIPOutputStream(new FileOutputStream(file, true));
//                     BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(gzipOut, StandardCharsets.UTF_8))) {
//                    for (JSONObject obj : jsonObjects) {
//                        writer.write(obj.toJSONString());
//                        writer.newLine();
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }


//                写入数据库MongoDB的某一个集合，取出每一个id的缓冲队列的元素，单节点部署
                MongoCollection<Document> collection = database1.getCollection(id);
//                collection.createIndex(Indexes.ascending("StartTime"));
                collection.createIndex(Indexes.ascending("checkTime"));

                List<Document> documents = new LinkedList<>();
                for (JSONObject jsonObject : jsonObjects) {
                    Document doc = Document.parse(jsonObject.toJSONString());
                    documents.add(doc);
                }
                collection.insertMany(documents);

                // 实时数据
                for (JSONObject jsonObject : jsonObjects) {
                    String name = jsonObject.getString("id");
                    if (name != null && !name.isEmpty()) {
                        String jsonDocument = jsonObject.toJSONString();

                        try (Jedis jedis = jedisPool.getResource()) {
                            jedis.select(2);
                            jedis.rpush(name, jsonDocument);
                            //60条数据，就是一分钟的数据量
                            jedis.ltrim(name, -60, -1);
                        }
                    }
                }


                // 写入算法计算需要，选择DB3
                for (JSONObject jsonObject : jsonObjects) {
                    String name = jsonObject.getString("id");
                    if (name != null && !name.isEmpty()) {
                        String jsonDocument = jsonObject.toJSONString();

                        try (Jedis jedis = jedisPool.getResource()) {
                            jedis.select(3);
                            jedis.rpush(name, jsonDocument);
                            //3600条数据 1小时
                            jedis.ltrim(name, -3600, -1);
                        }
                    }
                }


//                告警处理队列，放入db1，每次存入1200条数据，每次取出5分钟内数据（300条）进行比较，如果超过一半则生成告警记录
//                for (JSONObject jsonObject : jsonObjects) {
//                    String name = jsonObject.getString("id");
//                    if (name != null && !name.isEmpty()) {
//                        name = "warn" + name;
//                        String jsonDocument = jsonObject.toJSONString();
//
//                        try (Jedis jedis = jedisPool.getResource()) {
//                            jedis.select(1);
//                            jedis.rpush(name, jsonDocument);
//                            // 将列表修剪为最后1000个元素
//                            jedis.ltrim(name, -1200, -1);
//                        }
//                    }
//                }
            }

            // 4、清空缓冲队列的数据，迎接下一次的存入
            bufferMap.clear();
        } catch (Exception e) {
            e.printStackTrace(); // 打印异常信息，便于调试
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
