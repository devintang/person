package com.cmcc.iot.mongo;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 测试mongo db的连接
 * Created by Administrator on 2015/10/30.
 */
public class TestConnect {
    //test server list string
    private static String serverHostList = "localhost";
    private static int serverPort = 27017;

    private static List<MongoCredential> credentialList;
    private static List<ServerAddress> seeds;
    private MongoClient mongoClient;
    private DB db;
    static {
        credentialList = new ArrayList<>();
        seeds = new ArrayList<>();
        MongoCredential credential = MongoCredential.createCredential(
                "test","db_devcloud", "test".toCharArray());
        String[] hostList = serverHostList.split(",");
        for (String host : hostList) {
            try {
                seeds.add(new ServerAddress(host, serverPort));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            credentialList.add(credential);
        }
    }

    private static String dbName = "db_devcloud";
    private static String dpName = "test_yonghua";

    /**
     * connect mongo db
     */
    public TestConnect() {
        mongoClient = new MongoClient(seeds, credentialList);
        db = mongoClient.getDB(dbName);
    }

    /**
     * connect mongo db and set options
     * @param options mongo client option
     */
    public TestConnect(MongoClientOptions options) {
        mongoClient = new MongoClient(seeds, credentialList, options);
        db = mongoClient.getDB(dbName);
    }

    /**
     * insert some data into db
     */
    public void insertData() {
        DBObject value = new BasicDBObject("col1", "abc");
        value.put("time", new Date());
        db.getCollection(dpName).insert(value);
    }

    /**
     * close db connect
     */
    public void close() {
        mongoClient.close();
    }

    /**
     * test main interface
     * @param args  input arguments
     */
    public static void main(String[] args) {

//        TestConnect instance = new TestConnect();
        MongoClientOptions.Builder optionBuilder = new MongoClientOptions.Builder();
//        optionBuilder.connectTimeout(60);
        optionBuilder.socketTimeout(10);  //可控制连接的超时时长
        MongoClientOptions options = optionBuilder.build();
        TestConnect instance = new TestConnect(options);

        instance.insertData();
        Common.sleep(1000 * 9);
        instance.insertData();
        Common.sleep(1000 * 20);
        instance.insertData();
        Common.sleep(1000 * 10);
        instance.close();
    }
}
