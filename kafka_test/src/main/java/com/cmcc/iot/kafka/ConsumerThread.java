package com.cmcc.iot.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.log4j.Logger;

/**
 * 消费线程，处理消息
 * Created by yonghua on 2015/11/3.
 */
public class ConsumerThread implements Runnable {
    private static Logger log = Logger.getLogger(ConsumerThread.class);
    private String threadName;
    private KafkaStream<byte[], byte[]> stream;
    public ConsumerThread(KafkaStream kafkaStream, int threadIndex) {
        stream = kafkaStream;
        threadName = "[" + ServerProperties.TOPIC_NAME + ":" + threadIndex + "]";
    }

    public void run() {
        log.info(threadName + "start...");
        log.info("topic:" + ServerProperties.TOPIC_NAME);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        int i = 0;
        long timeStart = System.currentTimeMillis();
        while (true) {
            try {
                it.hasNext();
//                MessageAndMetadata<byte[], byte[]> msg = it.next();
//                if (msg.key() != null) {
//                    log.info(threadName + "msg:" + new String(msg.key()) + "," + new String(msg.message())
//                            + "," + msg.partition() + "," + msg.offset());
//                } else {
//                    log.info(threadName + "msg:null," + new String(msg.message())
//                            + "," + msg.partition() + "," + msg.offset());
//                }
                it.next();
                i++;
                if (i == 100000) {
                    long timeConsumer = System.currentTimeMillis() - timeStart;
                    timeStart = System.currentTimeMillis();
                    i = 0;

                    log.info(threadName + " consume 100000 time-consume:" + timeConsumer);
                }
            } catch (ConsumerTimeoutException e) {
                log.info(threadName + " timeout");
                break;
            }
        }
        log.info(threadName + "shutdown!!!");
    }
}
