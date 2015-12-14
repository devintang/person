package com.cmcc.iot.mongo;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试mongo replication set
 * Created by yonghua on 2015/11/4.
 */
public class TestReplicationSet {
    //test server list string
    private static String serverHostList = "192.168.200.218,192.168.200.218,192.168.200.218";
    private static String serverPortList = "27019,27018,27017";
    private static String DB_NAME = "db_devcloud";

    private static List<MongoCredential> credentialList;
    private static List<ServerAddress> seeds;
    private static MongoClientOptions mongoClientOptions;
    private MongoClient mongoClient;
    private DB db;
    static {
        credentialList = new ArrayList<>();
        seeds = new ArrayList<>();
        MongoCredential credential = MongoCredential.createCredential(
                "test","db_devcloud", "test".toCharArray());
        String[] hostList = serverHostList.split(",");
        String[] portList = serverPortList.split(",");
        for (int i = 0; i < hostList.length; i++) {
            try {
                seeds.add(new ServerAddress(hostList[i], Integer.parseInt(portList[i])));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            credentialList.add(credential);
        }

        //设置
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
//        builder.readPreference(ReadPreference.secondary());
        builder.readPreference(ReadPreference.secondaryPreferred());
//        builder.requiredReplicaSetName("baseinfo");
        mongoClientOptions = builder.build();
    }

    public TestReplicationSet() {
        mongoClient = new MongoClient(seeds, credentialList, mongoClientOptions);
        db = mongoClient.getDB(DB_NAME);
    }

    public void printOption() {
        Common.print(mongoClient.getReadPreference().toString());
        Common.print(mongoClient.getConnectPoint());
        Common.print(mongoClient.getReplicaSetStatus().toString());
        if (mongoClient.getAddress() == null) {
            Common.print("address:null");
        } else {
            Common.print(mongoClient.getAddress().toString());
        }
    }

    public void readData() {
        DBCollection table = db.getCollection("test_yonghua");
        DBCursor dbCursor = table.find().limit(5);
        while (dbCursor.hasNext()) {
            System.out.println(dbCursor.next().toString());
        }
    }

    public void close() {
        mongoClient.close();
    }

    public static void main(String[] args) {
        TestReplicationSet instance = new TestReplicationSet();
        instance.printOption();
        instance.readData();
        Common.sleep(2000);
        instance.printOption();
        instance.close();
    }
}
