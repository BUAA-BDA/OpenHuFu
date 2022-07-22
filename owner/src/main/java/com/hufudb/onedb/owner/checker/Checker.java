package com.hufudb.onedb.owner.checker;

import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;

public class Checker {

  static boolean checkLeaf(LeafPlan plan, Schema schema) {
    return true;
  }

  public static boolean check(Plan plan, SchemaManager manager) {
    for (Plan child : plan.getChildren()) {
      if (!check(child, manager)) {
        return false;
      }
    }
    if (plan.getPlanType().equals(PlanType.LEAF)) {
      if (checkLeaf((LeafPlan) plan, manager.getPublishedSchema(plan.getTableName()))) {
        return false;
      }
    }
    return true;
  }
}
