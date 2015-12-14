package com.cmcc.iot.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 发送消息线程，用于测试kafka写入效率
 * Created by yonghua on 2015/11/6.
 */
public class ProducerThread implements Runnable {
    private static Logger log = Logger.getLogger("ProducerThread");
    private static final String TOPIC_NAME = System.getProperty("topic");

    private final String THREAD_NAME;
    private int msgNum;
    private ProducerRecord msg;
    private KafkaProducer<byte[], byte[]> producer;

    public ProducerThread(int msgNum, int msgSize, int threadIndex) {
        THREAD_NAME = "[" + TOPIC_NAME + ":" + threadIndex + "]";
        this.msgNum = msgNum;
        byte[] msgBody = new byte[msgSize];
        for (int i = 0; i < msgSize; i++) {
            msgBody[i] = 'a';
        }
        msg = new ProducerRecord<byte[], byte[]>(ServerProperties.TOPIC_NAME,
                msgBody);

        Properties props = new Properties();
        props.put("bootstrap.servers", ServerProperties.KAFKA_BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<byte[], byte[]>(props);
    }

    public void run() {
        log.info(THREAD_NAME + "start...");
        long timeStart = System.currentTimeMillis();
        for (int i = 0; i < msgNum; i++) {
            producer.send(msg);
        }
        log.info(THREAD_NAME + " time-consume:" + (System.currentTimeMillis() - timeStart));
        log.info(THREAD_NAME + "shutdown!!!");
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("../conf/log4j.properties");
        if (args.length < 3) {
            log.error("need 3 arguments:[thread_num] [msg_num] [msg_size]");
            return;
        }

        int threadNum = Integer.parseInt(args[0]);
        int msgNum = Integer.parseInt(args[1]);
        int msgSize = Integer.parseInt(args[2]);
        ExecutorService executor = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
            executor.submit(new ProducerThread(msgNum, msgSize, i));
        }

    }
}
