package com.cmcc.iot.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 从kafka消费消息
 * Created by yonghua on 2015/11/3.
 */
public class Consumer {
    private static Logger log = Logger.getLogger(Consumer.class);
    private ConsumerConnector consumer;
    private ExecutorService executor;
    public Consumer() {
        this(ServerProperties.CONSUMER_GROUP_ID);
    }

    public Consumer(String consumerGroupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", ServerProperties.KAFKA_ZOOKEEPER);
        props.put("group.id", consumerGroupId);
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("consumer.timeout.ms", "30000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public void close() {
        log.info("consumer close.");
        if (consumer != null) {
            consumer.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    public void run(int threadNum) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(ServerProperties.TOPIC_NAME, threadNum);
        List<KafkaStream<byte[], byte[]>> streams = consumer.createMessageStreams(topicCountMap).get(
                ServerProperties.TOPIC_NAME);

        executor = Executors.newFixedThreadPool(threadNum);
        log.info("thread executor start...");
        int threadIndex = 0;
        for (KafkaStream stream : streams) {
            executor.submit(new ConsumerThread(stream, threadIndex));
            threadIndex++;
        }
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("../conf/log4j.properties");
        Consumer instance;
        if (args.length > 1) {
            instance = new Consumer(args[1]);
        } else {
            instance = new Consumer();
        }

        int threadNum = Integer.parseInt(args[0]);
        instance.run(threadNum);

        /*try {
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        instance.close();*/
//        ConsumerRead instance = new ConsumerRead();
//        instance.run();
    }
}
