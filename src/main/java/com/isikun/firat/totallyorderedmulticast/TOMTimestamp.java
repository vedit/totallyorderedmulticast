package com.isikun.firat.totallyorderedmulticast;

import java.util.logging.Logger;

/**
 * Created by hexenoid on 12/27/14.
 */
public class TOMTimestamp {
    static volatile int time;

    private static volatile TOMTimestamp instance = null;
    private final static Logger LOGGER = Logger.getLogger("TOMTimestamp");

    public static TOMTimestamp getInstance() {
        if (instance == null) {
            synchronized (TOMTimestamp.class) {
                if (instance == null) {
                    instance = new TOMTimestamp();
                }
            }
        }
        return instance;
    }

    private TOMTimestamp(){
        time = 0;
    }
    public static int getTime() {
        return time;
    }

    public static void setTime(int time) {
        if (time > TOMTimestamp.time) {
            LOGGER.info("Adjusting local clock");
            TOMTimestamp.time = time;
        } else {
            LOGGER.info("Clock not modified");
        }
        LOGGER.info("Clock: " + TOMTimestamp.time);
    }

    public static void increment() {
        time += 1;
        LOGGER.info("Clock: " + TOMTimestamp.time);
    }
}
