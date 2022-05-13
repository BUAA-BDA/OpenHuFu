package com.hufudb.onedb.plan;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueryPlanPool {
  static final Map<Long, Plan> contexts = new ConcurrentHashMap<>();

  public static Plan getContext(long cid) {
    return contexts.get(cid);
  }

  public static void savePlan(RootPlan plan) {
    contexts.put(plan.getPlanId(), plan);
  }

  public static void deleteContext(long id) {
    contexts.remove(id);
  }
}
