package com.yh.hadoop;

import com.yh.hadoop.hbase.HbaseTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * hadoop 相关工具
 */
public class Tool {
    private final static Logger logger = LoggerFactory.getLogger(Tool.class);

    public static void printHelp() {
        logger.info("hadoop tool help information\n"
                + "COMMAND GROUPS:\n"
                + "Group name: hdfs\n"
                + "Commands: (now is null)\n"
                + "Group name: hbase\n"
                + "Commands: deleteMany, scan");
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            logger.info("arguments invalid.");
            printHelp();
            return;
        }
        if (args[0].equals("help")) {
            printHelp();
            return;
        }
        switch (args[0]) {
            case "hbase":
                if (args.length < 4) {
                    logger.info("need more arguments:\n" +
                            "(example)hbase zookeeper_hosts zookeeper_port command...");
                    return;
                }
                HbaseTool hbaseTool;
                try {
                    hbaseTool = new HbaseTool(args[1], args[2]);
                } catch (IOException e) {
                    logger.error("init hbaseTool exception", e);
                    return;
                }
                try {
                    hbaseTool.process(args);
                } catch (IOException e) {
                    logger.error("hbase tool exception", e);
                } finally {
                    hbaseTool.close();
                }
                break;
            case "hdfs":
                logger.info("now hdfs is no command...");
                break;
            default:
                logger.info("not support command group.command group:" + args[0]);
                printHelp();
                break;
        }
    }
}
