package com.cmcc.iot.mongo;

/**
 * 常用方法
 * Created by Administrator on 2015/11/2.
 */
public class Common {
    public static void sleep(long sleepTimeMsec) {
        try {
            Thread.sleep(sleepTimeMsec);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void print(String log) {
        System.out.println(log);
    }
}
