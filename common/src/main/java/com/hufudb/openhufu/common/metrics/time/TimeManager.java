package com.hufudb.openhufu.common.metrics.time;

import java.util.HashMap;
import java.util.Map;

public class TimeManager {
    private static Map<String, Long> timeMap = new HashMap<>();

    public static void addTimeInfo(String name, long execTime) {
        timeMap.put(name,execTime);
    }

    public static long getTimeInfo(String name) {
        return timeMap.get(name);
    }

    public static Map<String, Long> getAllInfo() {
        return timeMap;
    }
}
