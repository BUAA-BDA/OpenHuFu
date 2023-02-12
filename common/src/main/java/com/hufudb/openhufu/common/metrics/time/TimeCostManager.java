package com.hufudb.openhufu.common.metrics.time;

import com.hufudb.openhufu.common.enums.TimeTerm;
import java.util.HashMap;
import java.util.Map;

public class TimeCostManager {
  private static final Map<TimeTerm, Long> timeCostMap = new HashMap<>();

  public static void addTimeCostInfo(TimeTerm term, Long execTime) {
    timeCostMap.put(term, execTime);
  }

  public static Long getTimeCostInfo(TimeTerm term) {
    return timeCostMap.get(term);
  }

  public static Map<TimeTerm, Long> getAllTimeCostInfo() {
    return timeCostMap;
  }
}
