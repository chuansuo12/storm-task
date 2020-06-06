package edu.big.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;


public class SaveDataBolt implements IRichBolt {
    private static final byte[] USER_INFO = Bytes.toBytes("user_info");
    private static final byte[] ARTICLE_INFO = Bytes.toBytes("article_info");
    private static final byte[] BEHAVIOR = Bytes.toBytes("behavior");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.0");

    private Table userBehaviorTable;
    private Table articleBehaviorTable;

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(conf);
            TableName userBehavior = TableName.valueOf("bigdata:user_behavior");
            TableName articleBehavior = TableName.valueOf("bigdata:article_behavior");
            this.userBehaviorTable = conn.getTable(userBehavior);
            this.articleBehaviorTable = conn.getTable(articleBehavior);
	    System.out.println("connect hbase success!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        try {
	    //System.out.println("execute...");
            UserBehavior userBehavior = getUserBehavior(tuple);
            Put userPut = this.getUserBehaviorPut(userBehavior);
            Put articlePut = this.getArticleBehaviorPut(userBehavior);
            userBehaviorTable.put(userPut);
            articleBehaviorTable.put(articlePut);
            //System.out.println("save success, uid:{" + userBehavior.getUid() + "}, aid:{" + userBehavior.getAid() + "}");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Put getUserBehaviorPut(UserBehavior userBehavior) {
        Put put = new Put(Bytes.toBytes(userBehavior.getUid()));
        put.addColumn(ARTICLE_INFO, UserBehavior.AID_FILE, Bytes.toBytes(userBehavior.getAid()));
        put.addColumn(BEHAVIOR, UserBehavior.BEHAVIOR_FILED, Bytes.toBytes(userBehavior.getBehavior()));
        put.setTimestamp(userBehavior.getBehaviorTime().atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli());
        return put;
    }

    private Put getArticleBehaviorPut(UserBehavior userBehavior) {
        Put put = new Put(Bytes.toBytes(userBehavior.getBehavior()));
        put.addColumn(USER_INFO, UserBehavior.UID_FILED, Bytes.toBytes(userBehavior.getUid()));
        put.addColumn(BEHAVIOR, UserBehavior.BEHAVIOR_FILED, Bytes.toBytes(userBehavior.getBehavior()));
        put.setTimestamp(userBehavior.getBehaviorTime().atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli());
        return put;
    }

    private UserBehavior getUserBehavior(Tuple tuple) {
        String value = tuple.getStringByField("value");
        String[] values = value.split("\001");
        UserBehavior userBehavior = new UserBehavior();
        userBehavior.setUid(values[0]);
        userBehavior.setBehavior(values[1]);
        userBehavior.setAid(values[2]);
        userBehavior.setBehaviorTime(LocalDateTime.parse(values[3], DATE_TIME_FORMATTER));
        return userBehavior;
    }

    public void cleanup() {
        try {
            userBehaviorTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            articleBehaviorTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
