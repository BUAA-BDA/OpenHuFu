package com.hufudb.onedb.plan;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueryPlanPool {
  static final Map<Long, Plan> plans = new ConcurrentHashMap<>();

  public static Plan getPlan(long cid) {
    return plans.get(cid);
  }

  public static void savePlan(RootPlan plan) {
    plans.put(plan.getPlanId(), plan);
  }

  public static void deletePlan(long id) {
    plans.remove(id);
  }
}
