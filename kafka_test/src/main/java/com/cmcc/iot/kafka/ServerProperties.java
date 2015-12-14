package com.cmcc.iot.kafka;

/**
 * kafka配置参数
 * Created by yonghua on 2015/11/3.
 */
public class ServerProperties {
//    private final static String DEFAULT_TOPIC_NAME = "test_p10";            //默认topic名称
    public final static String TOPIC_NAME = System.getProperty("topic");;     //消息topic名称
//    public final static String KAFKA_BROKER_LIST = "192.168.200.218:9092";  //kafka broker列表
    public final static String KAFKA_BROKER_LIST = "192.168.27.13:9092,192.168.27.14:9092,192.168.27.15:9092";
//    public final static String KAFKA_ZOOKEEPER = "192.168.200.218:2181";    //kafka zookeeper连接信息
    public final static String KAFKA_ZOOKEEPER = "192.168.27.28:2181";    //kafka zookeeper连接信息
    public final static String CONSUMER_GROUP_ID = "test";      //kafka消费组ID
    public final static String CONSUMER_TIMEOUT = "500";                       //kafka消费超时时长
}
