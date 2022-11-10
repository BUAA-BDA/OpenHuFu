package com.hufudb.onedb.core.sql.plan;

import java.util.ArrayList;
import java.util.List;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.plan.EmptyPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;
import com.hufudb.onedb.proto.OneDBPlan.QueryPlanProto;
import com.hufudb.onedb.proto.OneDBPlan.TaskInfo;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class PlanUtils {
  public static List<Pair<OwnerClient, QueryPlanProto>> generateOwnerPlans(OneDBClient client, Plan plan) {
    PlanType type = plan.getPlanType();
    switch(type) {
      case ROOT:
        return generateOwnerPlans(client, plan.getChildren().get(0));
      case LEAF:
        return generateLeafOwnerPlans(client, plan);
      case UNARY:
        return generateUnaryOwnerPlans(client, plan);
      case BINARY:
        return generateBinaryOwnerPlans(client, plan);
      default:
        throw new UnsupportedOperationException("Unsupported plan type");
    }
  }

  public static List<Pair<OwnerClient, QueryPlanProto>> generateLeafOwnerPlans(OneDBClient client, Plan plan) {
    QueryPlanProto.Builder builder =
    QueryPlanProto.newBuilder().setType(PlanType.LEAF)
        .addAllSelectExp(plan.getSelectExps()).setFetch(plan.getFetch()).setOffset(plan.getOffset());
    if (!plan.getWhereExps().isEmpty()) {
      builder.addAllWhereExp(plan.getWhereExps());
    }
    if (!plan.getAggExps().isEmpty()) {
      builder.addAllAggExp(plan.getAggExps());
    }
    if (!plan.getGroups().isEmpty()) {
      builder.addAllGroup(plan.getGroups());
    }
    if (!plan.getOrders().isEmpty()) {
      builder.addAllOrder(plan.getOrders());
    }
    List<Pair<OwnerClient, String>> tableClients = client.getTableClients(plan.getTableName());
    List<Pair<OwnerClient, QueryPlanProto>> ownerContext = new ArrayList<>();
    for (Pair<OwnerClient, String> entry : tableClients) {
      builder.setTableName(entry.getRight());
      ownerContext.add(MutablePair.of(entry.getLeft(), builder.build()));
    }
    return ownerContext;
  }

  public static List<Pair<OwnerClient, QueryPlanProto>> generateUnaryOwnerPlans(OneDBClient client, Plan plan) {
    QueryPlanProto.Builder builder = QueryPlanProto.newBuilder().setType(PlanType.UNARY).setFetch(plan.getFetch()).setOffset(plan.getOffset());
    if (!plan.getSelectExps().isEmpty()) {
      builder.addAllSelectExp(plan.getSelectExps());
    }
    if (!plan.getAggExps().isEmpty()) {
      builder.addAllAggExp(plan.getAggExps());
    }
    if (!plan.getGroups().isEmpty()) {
      builder.addAllGroup(plan.getGroups());
    }
    if (!plan.getOrders().isEmpty()) {
      builder.addAllOrder(plan.getOrders());
    }
    List<Pair<OwnerClient, QueryPlanProto>> ownerPlan = generateOwnerPlans(client, plan.getChildren().get(0));
    // todo: generate task info for each expression
    TaskInfo.Builder taskInfo = TaskInfo.newBuilder().setTaskId(client.getTaskId());
    for (Pair<OwnerClient, QueryPlanProto> p : ownerPlan) {
      taskInfo.addParties(p.getLeft().getParty().getPartyId());
    }
    builder.setTaskInfo(taskInfo);
    for (Pair<OwnerClient, QueryPlanProto> p : ownerPlan) {
      QueryPlanProto pl = builder.addChildren(p.getValue()).build();
      p.setValue(pl);
      builder.clearChildren();
    }
    return ownerPlan;
  }

  public static List<Pair<OwnerClient, QueryPlanProto>> generateBinaryOwnerPlans(OneDBClient client, Plan plan) {
    QueryPlanProto.Builder builder = QueryPlanProto.newBuilder().setType(PlanType.BINARY).setFetch(plan.getFetch()).setOffset(plan.getOffset());
    if (!plan.getSelectExps().isEmpty()) {
      builder.addAllSelectExp(plan.getSelectExps());
    }
    if (!plan.getAggExps().isEmpty()) {
      builder.addAllAggExp(plan.getAggExps());
    }
    if (!plan.getGroups().isEmpty()) {
      builder.addAllGroup(plan.getGroups());
    }
    if (!plan.getOrders().isEmpty()) {
      builder.addAllOrder(plan.getOrders());
    }
    Plan left = plan.getChildren().get(0);
    Plan right = plan.getChildren().get(1);
    List<Pair<OwnerClient, QueryPlanProto>> leftPlan = generateOwnerPlans(client, left);
    List<Pair<OwnerClient, QueryPlanProto>> rightPlan = generateOwnerPlans(client, right);
    List<Pair<OwnerClient, QueryPlanProto>> ownerPlan = new ArrayList<>();
    TaskInfo.Builder taskInfo = TaskInfo.newBuilder().setTaskId(client.getTaskId());
    for (Pair<OwnerClient, QueryPlanProto> p : leftPlan) {
      taskInfo.addParties(p.getLeft().getParty().getPartyId());
    }
    for (Pair<OwnerClient, QueryPlanProto> p : rightPlan) {
      taskInfo.addParties(p.getLeft().getParty().getPartyId());
    }
    builder.setTaskInfo(taskInfo);
    QueryPlanProto leftPlaceholder = new EmptyPlan(left.getOutExpressions()).toProto();
    QueryPlanProto rightPlaceholder = new EmptyPlan(right.getOutExpressions()).toProto();
    // for owners from left
    builder.setJoinInfo(plan.getJoinCond().toBuilder().build());
    for (Pair<OwnerClient, QueryPlanProto> p : leftPlan) {
      QueryPlanProto context = builder.addChildren(0, p.getValue())
          .addChildren(1, rightPlaceholder).build();
      p.setValue(context);
      ownerPlan.add(p);
    }
    // for owners from right
    builder.setJoinInfo(plan.getJoinCond());
    for (Pair<OwnerClient, QueryPlanProto> p : rightPlan) {
      QueryPlanProto context =
          builder.setChildren(0, leftPlaceholder)
              .setChildren(1, p.getValue()).build();
      p.setValue(context);
      ownerPlan.add(p);
    }
    return ownerPlan;
  }
}
