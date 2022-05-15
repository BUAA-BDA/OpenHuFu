package com.hufudb.onedb.plan;

import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.expression.ExpressionUtils;
import com.hufudb.onedb.implementor.PlanImplementor;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Collation;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;
import com.hufudb.onedb.proto.OneDBPlan.QueryPlanProto;
import com.hufudb.onedb.proto.OneDBPlan.TaskInfo;
import com.hufudb.onedb.rewriter.Rewriter;

/**
 * Plan for intermediate process with single input relation 
 * (e.g., outer layer of nested aggregation)
 */
public class UnaryPlan extends BasePlan {
  Plan child;
  List<Expression> aggExps = ImmutableList.of();
  List<Expression> selectExps = ImmutableList.of();
  List<Integer> groups = ImmutableList.of();
  List<Collation> orders = ImmutableList.of();
  int fetch;
  int offset;
  TaskInfo taskInfo;

  public UnaryPlan() {
    super();
  }

  public static UnaryPlan fromProto(QueryPlanProto proto) {
    UnaryPlan plan = new UnaryPlan();
    plan.setChildren(ImmutableList.of(Plan.fromProto(proto.getChildren(0))));
    plan.setAggExps(proto.getAggExpList());
    plan.setSelectExps(proto.getSelectExpList());
    plan.setGroups(proto.getGroupList());
    plan.setOrders(proto.getOrderList());
    plan.setFetch(proto.getFetch());
    plan.setOffset(proto.getOffset());
    plan.taskInfo = proto.getTaskInfo();
    return plan;
  }

  @Override
  public List<Plan> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public void setChildren(List<Plan> children) {
    assert children.size() == 1;
    this.child = children.get(0);
  }

  @Override
  public void updateChild(Plan newChild, Plan oldChild) {
    assert oldChild == child;
    child = newChild;
  }

  @Override
  public List<Expression> getOutExpressions() {
    if (aggExps != null && !aggExps.isEmpty()) {
      return aggExps;
    } else if (selectExps != null && !selectExps.isEmpty()) {
      return selectExps;
    } else {
      LOG.error("Unary plan without output expression");
      throw new RuntimeException("Unary plan without output expression");
    }
  }

  @Override
  public List<Expression> getAggExps() {
    return aggExps;
  }

  @Override
  public void setAggExps(List<Expression> aggExps) {
    this.aggExps = aggExps;
  }
  
  @Override
  public List<Expression> getSelectExps() {
    return selectExps;
  }

  @Override
  public void setSelectExps(List<Expression> selectExps) {
    this.selectExps = selectExps;
  }

  @Override
  public List<Integer> getGroups() {
    return groups;
  }

  @Override
  public void setGroups(List<Integer> groups) {
    this.groups = groups;
  }

  @Override
  public List<Collation> getOrders() {
    return orders;
  }

  @Override
  public void setOrders(List<Collation> orders) {
    this.orders = orders;
  }

  @Override
  public List<ColumnType> getOutTypes() {
    return getOutExpressions().stream()
        .map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public Modifier getPlanModifier() {
    return ModifierWrapper.dominate(getOutModifiers());
  }

  @Override
  public List<Modifier> getOutModifiers() {
    return getOutExpressions().stream().map(exp -> exp.getModifier()).collect(Collectors.toList());
  }

  @Override
  public int getFetch() {
    return fetch;
  }

  @Override
  public void setFetch(int fetch) {
    this.fetch = fetch;
  }

  @Override
  public int getOffset() {
    return offset;
  }

  @Override
  public void setOffset(int offset) {
    this.offset = offset;
  }

  @Override
  public PlanType getPlanType() {
    return PlanType.UNARY;
  }

  @Override
  public TaskInfo getTaskInfo() {
    return taskInfo;
  }

  @Override
  public DataSet implement(PlanImplementor implementor) {
    return implementor.unaryQuery(this);
  }

  @Override
  public Plan rewrite(Rewriter rewriter) {
    this.child = child.rewrite(rewriter);
    return rewriter.rewriteUnary(this);
  }

  @Override
  public Schema getOutSchema() {
    return ExpressionUtils.createSchema(getOutExpressions());
  }
}
