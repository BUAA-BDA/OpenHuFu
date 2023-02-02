package com.hufudb.openhufu.plan;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.utils.ModifierWrapper;
import com.hufudb.openhufu.expression.ExpressionUtils;
import com.hufudb.openhufu.implementor.PlanImplementor;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Collation;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinCondition;
import com.hufudb.openhufu.proto.OpenHuFuPlan.PlanType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.QueryPlanProto;
import com.hufudb.openhufu.proto.OpenHuFuPlan.TaskInfo;
import com.hufudb.openhufu.rewriter.Rewriter;

/**
 * plan for join
 */
public class BinaryPlan extends BasePlan {
  Plan left;
  Plan right;
  List<Expression> selectExps = ImmutableList.of();
  List<Expression> whereExps = ImmutableList.of();
  List<Expression> aggExps = ImmutableList.of();
  List<Integer> groups = ImmutableList.of();
  List<Collation> orders = ImmutableList.of();
  int fetch;
  int offset;
  JoinCondition joinCond;
  TaskInfo taskInfo;

  public BinaryPlan(Plan left, Plan right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public List<Plan> getChildren() {
    return ImmutableList.of(left, right);
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
  public List<Expression> getWhereExps() {
    return whereExps;
  }

  @Override
  public void setWhereExps(List<Expression> whereExps) {
    this.whereExps = whereExps;
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
    return getOutExpressions().stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public Modifier getPlanModifier() {
    return ModifierWrapper.dominate(ModifierWrapper.dominate(getOutModifiers()),
        joinCond.getModifier());
  }

  @Override
  public List<Modifier> getOutModifiers() {
    return getOutExpressions().stream().map(exp -> exp.getModifier()).collect(Collectors.toList());
  }

  @Override
  public List<Expression> getOutExpressions() {
    if (aggExps != null && !aggExps.isEmpty()) {
      return aggExps;
    } else if (selectExps != null && !selectExps.isEmpty()) {
      return selectExps;
    } else {
      LOG.error("Binary plan without output expression");
      throw new RuntimeException("Binary plan without output expression");
    }
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
  public JoinCondition getJoinCond() {
    return joinCond;
  }

  @Override
  public void setJoinInfo(JoinCondition joinCond) {
    this.joinCond = joinCond;
  }

  @Override
  public PlanType getPlanType() {
    return PlanType.BINARY;
  }

  @Override
  public TaskInfo getTaskInfo() {
    return taskInfo;
  }

  @Override
  public Schema getOutSchema() {
    return ExpressionUtils.createSchema(getOutExpressions());
  }

  @Override
  public DataSet implement(PlanImplementor implementor) {
    return implementor.binaryQuery(this);
  }

  @Override
  public Plan rewrite(Rewriter rewriter) {
    this.left = left.rewrite(rewriter);
    this.right = right.rewrite(rewriter);
    return rewriter.rewriteBinary(this);
  }

  @Override
  public String toString() {
    return "BinaryPlan {" + '\n' +
        ("\tselectExps=" + selectExps + '$' +
            "\twhereExps=" + whereExps + '$' +
            "\taggExps=" + aggExps + '$' +
            "\tgroups=" + groups + '$' +
            "\torders=" + orders + '$' +
            "\tfetch=" + fetch + '$' +
            "\toffset=" + offset + '$' +
            "\tjoinCond=" + joinCond + '$' +
            "\ttaskInfo=" + taskInfo).replace('\n', '|').replace('$', '\n') + "\n" +
        "\tleft->" + left.toString().replace("\n", "\n\t") + "\n" +
        "\tright->" + right.toString().replace("\n", "\n\t") + "\n" +
        "}";
  }

  public static BinaryPlan fromProto(QueryPlanProto proto) {
    BinaryPlan plan =
        new BinaryPlan(Plan.fromProto(proto.getChildren(0)), Plan.fromProto(proto.getChildren(1)));
    plan.setSelectExps(proto.getSelectExpList());
    plan.setWhereExps(proto.getWhereExpList());
    plan.setAggExps(proto.getAggExpList());
    plan.setGroups(proto.getGroupList());
    plan.setOrders(proto.getOrderList());
    plan.setFetch(proto.getFetch());
    plan.setOffset(proto.getOffset());
    plan.setJoinInfo(proto.getJoinInfo());
    plan.taskInfo = proto.getTaskInfo();
    return plan;
  }
}
