package com.cmcc.iot.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 发送消息到kafka
 * Created by yonghua on 2015/11/3.
 */
public class Producer {
    private KafkaProducer<byte[], byte[]> producer;

    /**
     * 创建与kafka的连接
     */
    public Producer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", ServerProperties.KAFKA_BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<byte[], byte[]>(props);
    }

    /**
     * 发送消息
     * @param msgNum
     */
    public void sendMsg(int msgNum) {
        for (int i = 0; i < msgNum; i++) {
            ProducerRecord msg = new ProducerRecord<byte[], byte[]>(ServerProperties.TOPIC_NAME,
                    (Integer.toString(10000 + i)).getBytes());
            Future<RecordMetadata> res = producer.send(msg);
        }
    }

    /**
     * 发送消息，并指定消息的key
     * @param msgNum
     */
    public void sendMsgSetKey(int msgNum) {
        for (int i = 0; i < msgNum; i++) {
            ProducerRecord msg = new ProducerRecord<byte[], byte[]>(ServerProperties.TOPIC_NAME,
                    ("k_" + i).getBytes(), (Integer.toString(10000 + i)).getBytes());
            producer.send(msg);
        }
    }

    /**
     * 发送消息，并指定分区
     * @param msgNum
     */
    public void sendMsgSetPartition(int msgNum) {
        for (int i = 0, p = 0; i < msgNum; i++,p++) {
            ProducerRecord msg = new ProducerRecord<byte[], byte[]>(ServerProperties.TOPIC_NAME,
                    p, ("p_" + i).getBytes(), (Integer.toString(10000 + i)).getBytes());
            producer.send(msg);
            if (p == 9) {
                p = 0;
            }
        }
    }

    public void close() {
        producer.close();
    }

    /**
     * producer启动入口
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("need 2 arguments:[msg_type] [msg_num]");
            return;
        }

        String msgType = args[0];
        int msgNum = Integer.parseInt(args[1]);
        Producer instance = new Producer();
        if (msgType.equals("msg")) {
            instance.sendMsg(msgNum);
        } else if (msgType.equals("key_msg")) {
            instance.sendMsgSetKey(msgNum);
        } else if (msgType.equals("partition_msg")) {
            instance.sendMsgSetPartition(msgNum);
        } else {
            System.out.println("not support msg type:" + msgType);
        }

        instance.close();
    }
}
