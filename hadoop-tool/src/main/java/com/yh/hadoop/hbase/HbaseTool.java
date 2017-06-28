package com.yh.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase tool
 * Created by yonghua on 2017/6/27.
 */
public class HbaseTool {
    private final static Logger logger = LoggerFactory.getLogger(HbaseTool.class);
    private Connection connection;

    public static void printHelp() {
        logger.info("command hbase help information\n"
                + "deleteMany:\n"
                + "Delete all cells or rows in a given condition; pass a table name, rowkey range\n"
                + ", family name and column name.Examples:\n"
                + "deleteMany t1 f1:c1 start_row1\n"
                + "deleteMany t1 f1:c1 start_row1 end_row1\n"
                + "scan:\n"
                + "Scan a table; pass a table name, rowkey range, limit, famliy column name.Examples:\n"
                + "scan t1 start_row1 5\n" +
                "scan t1 start_row1 5 f1:c1"
        );
    }

    public HbaseTool(String hosts, String port) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", port);
        configuration.set("hbase.zookeeper.quorum", hosts);
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    public void process(String[] args) throws IOException {
        String command = args[3];
        if (command.equals("deleteMany")) {
            deleteMany(args);
        } else if (command.equals("scan")) {
            scan(args);
        }
    }

    public void scan(String[] args) throws IOException {
        if (args.length < 7) {
            printHelp();
            return;
        }

        Scan scan = new Scan();
        scan.setStartRow(args[5].getBytes());
        scan.setFilter(new PageFilter(Integer.parseInt(args[6])));
        if (args.length > 7) {
            String[] familyColumn = args[7].split(":");
            if (familyColumn.length != 2) {
                logger.warn("family column invalid");
                printHelp();
                return;
            }
            scan.addColumn(familyColumn[0].getBytes(), familyColumn[1].getBytes());
        }

        Table table = connection.getTable(TableName.valueOf(args[4]));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            logger.info("rowkey:" + new String(result.getRow()));
        }
        scanner.close();
    }

    public void deleteMany(String[] args) throws IOException {
        if (args.length < 7) {
            printHelp();
            return;
        }

        Scan scan = new Scan();
        scan.setCaching(100);
        scan.setStartRow(args[6].getBytes());
        if (args.length > 7) {
            scan.setStopRow(args[7].getBytes());
        }
        String[] familyColumn = args[5].split(":");
        if (familyColumn.length != 2) {
            logger.warn("family column invalid");
            printHelp();
            return;
        }
        scan.addColumn(familyColumn[0].getBytes(), familyColumn[1].getBytes());
        String tableName = args[4];

        Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        int cnt = 0;
        List<Delete> deleteList = new ArrayList<>();
        for (Result result : scanner) {
            cnt++;
            deleteList.add(new Delete(result.getRow()));

            if (deleteList.size() >= 50) {
                table.delete(deleteList);
                deleteList.clear();
            }
            if (cnt % 10000 == 0) {
                logger.info("complete delete row number:" + cnt);
            }
        }
        scanner.close();
        if (deleteList.size() > 0) {
            table.delete(deleteList);
            deleteList.clear();
        }
        logger.info("finish delete row number:" + cnt);
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.warn("hbase tool close exception", e);
            }
        }
    }
}
