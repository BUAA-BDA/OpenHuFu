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
import com.hufudb.onedb.rewriter.Rewriter;

/**
 * Plan for single global table query (horizontal partitioned table)
 */
public class LeafPlan extends BasePlan {
  String tableName;
  List<Expression> selectExps = ImmutableList.of();
  List<Expression> whereExps = ImmutableList.of();
  List<Expression> aggExps = ImmutableList.of();
  List<Integer> groups = ImmutableList.of();
  List<Collation> orders = ImmutableList.of();
  int fetch;
  int offset;

  public LeafPlan() {
    super();
  }

  public QueryPlanProto toProto() {
    QueryPlanProto.Builder builder = QueryPlanProto.newBuilder();
    builder.setType(PlanType.LEAF).setTableName(tableName)
        .addAllSelectExp(selectExps).setFetch(fetch).setOffset(offset);
    builder.addAllWhereExp(whereExps);
    builder.addAllAggExp(aggExps);
    builder.addAllGroup(groups);
    builder.addAllOrder(orders);
    return builder.build();
  }

  public static LeafPlan fromProto(QueryPlanProto proto) {
    LeafPlan plan = new LeafPlan();
    plan.setTableName(proto.getTableName());
    plan.setSelectExps(proto.getSelectExpList());
    plan.setWhereExps(proto.getWhereExpList());
    plan.setAggExps(proto.getAggExpList());
    plan.setGroups(proto.getGroupList());
    plan.setOrders(proto.getOrderList());
    plan.setFetch(proto.getFetch());
    plan.setOffset(proto.getOffset());
    return plan;
  }

  @Override
  public Schema getOutSchema() {
    return ExpressionUtils.createSchema(getOutExpressions());
  }

  @Override
  public PlanType getPlanType() {
    return PlanType.LEAF;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void setTableName(String name) {
    this.tableName = name;
  }

  @Override
  public List<ColumnType> getOutTypes() {
    return getOutExpressions().stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
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
  public List<Expression> getOutExpressions() {
    if (!aggExps.isEmpty()) {
      return aggExps;
    } else if (!selectExps.isEmpty()) {
      return selectExps;
    } else {
      LOG.error("Leaf Plan without output expression");
      throw new RuntimeException("Leaf Plan without output expression");
    }
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
  public boolean hasAgg() {
    return !aggExps.isEmpty();
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

  public List<ColumnType> getSelectTypes() {
    return selectExps.stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public DataSet implement(PlanImplementor implementor) {
    return implementor.leafQuery(this);
  }

  @Override
  public Plan rewrite(Rewriter rewriter) {
    return rewriter.rewriteLeaf(this);
  }

  @Override
  public String toString() {
    return "LeafPlan{" + '\n' +
        ("\ttableName='" + tableName + '\'' + '$' +
            "\tselectExps=" + selectExps + '$' +
            "\twhereExps=" + whereExps + '$' +
            "\taggExps=" + aggExps + '$' +
            "\tgroups=" + groups + '$' +
            "\torders=" + orders + '$' +
            "\tfetch=" + fetch + '$' +
            "\toffset=" + offset).replace('\n', '|').replace('$', '\n') + '\n' +
        "}";
  }
}
