package com.cmcc.iot.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * 测试kafka读取数据的可用性
 * Created by yonghua on 2015/11/5.
 */
public class ConsumerRead {
    private static Logger log = Logger.getLogger(ConsumerRead.class);
    private ConsumerConnector connector;
    public ConsumerRead() {
        Properties props = new  Properties();
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("zookeeper.connect", ServerProperties.KAFKA_ZOOKEEPER);
        props.put("group.id", "test");
        props.put("consumer.timeout.ms", "10000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        connector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public void run() {
        String topic = System.getProperty("topic");
        HashMap<String, Integer> consumerMap = new HashMap<String, Integer>();
        consumerMap.put(topic, 1);
        List<KafkaStream<byte[], byte[]>> dataStreams =
                connector.createMessageStreams(consumerMap).get(topic);
        KafkaStream<byte[], byte[]> stream = dataStreams.get(0);
        ConsumerIterator<byte[], byte[]> dataIte = stream.iterator();
        while (true) {
            try {
                dataIte.hasNext();
                MessageAndMetadata<byte[], byte[]> msg = dataIte.next();
                if (msg.key() != null) {
                    log.info("consumer-read-msg:" + new String(msg.key()) + "," + new String(msg.message())
                            + "," + msg.partition() + "," + msg.offset());
                } else {
                    log.info("consumer-read-msg:" + "null," + new String(msg.message())
                            + "," + msg.partition() + "," + msg.offset());
                }
            } catch(ConsumerTimeoutException e) {
                log.info("timeout");
                break;
            }
        }
        log.info("consumer-read exit!!!");
    }
}
