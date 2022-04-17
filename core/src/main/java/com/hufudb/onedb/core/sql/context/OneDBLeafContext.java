package com.hufudb.onedb.core.sql.context;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.implementor.OneDBImplementor;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;

/*
 * context for single global table query (horizontal partitioned table)
 */
public class OneDBLeafContext extends OneDBBaseContext {
  OneDBContext parent;
  OneDBContextType contextType;
  String tableName;
  List<OneDBExpression> selectExps = new ArrayList<>();
  List<OneDBExpression> whereExps = new ArrayList<>();
  List<OneDBExpression> aggExps = new ArrayList<>();
  List<Integer> groups = new ArrayList<>();
  List<String> orders = new ArrayList<>();
  int fetch;
  int offset;

  public OneDBLeafContext() {
    super();
  }

  OneDBLeafContext(OneDBQueryProto proto) {
    super();
    this.tableName = proto.getTableName();
    this.selectExps = OneDBExpression.fromProto(proto.getSelectExpList());
    this.whereExps = OneDBExpression.fromProto(proto.getWhereExpList());
    this.aggExps = OneDBExpression.fromProto(proto.getAggExpList());
    this.groups = proto.getGroupList();
    this.orders = proto.getOrderList();
    this.fetch = proto.getFetch();
    this.offset = proto.getOffset();
    LOG.warn("should not be here");
  }

  public static OneDBContext fromProto(OneDBQueryProto proto) {
    return new OneDBLeafContext(proto);
  }

  public OneDBQueryProto toProto() {
    OneDBQueryProto.Builder builder = OneDBQueryProto.newBuilder();
    builder.setTableName(tableName).addAllSelectExp(OneDBExpression.toProto(selectExps))
        .setFetch(fetch).setOffset(offset);
    if (whereExps != null) {
      builder.addAllWhereExp(OneDBExpression.toProto(whereExps));
    }
    if (aggExps != null) {
      builder.addAllAggExp(OneDBExpression.toProto(aggExps));
    }
    if (groups != null) {
      builder.addAllGroup(groups);
    }
    if (orders != null) {
      builder.addAllOrder(orders);
    }
    return builder.build();
  }

  @Override
  public OneDBContextType getContextType() {
    return OneDBContextType.LEAF;
  }

  @Override
  public OneDBContext getParent() {
    return parent;
  }

  @Override
  public void setParent(OneDBContext parent) {
    this.parent = parent;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public List<FieldType> getOutTypes() {
    return getOutExpressions().stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public List<OneDBExpression> getOutExpressions() {
    if (aggExps != null && !aggExps.isEmpty()) {
      return aggExps;
    } else if (selectExps != null && !selectExps.isEmpty()) {
      return selectExps;
    } else {
      LOG.error("Leaf context without output expression");
      throw new RuntimeException("Leaf context without output expression");
    }
  }

  @Override
  public List<OneDBExpression> getSelectExps() {
    return selectExps;
  }

  @Override
  public void setSelectExps(List<OneDBExpression> selectExps) {
    this.selectExps = selectExps;
  }

  @Override
  public List<OneDBExpression> getWhereExps() {
    return whereExps;
  }

  @Override
  public void setWhereExps(List<OneDBExpression> whereExps) {
    this.whereExps = whereExps;
  }

  @Override
  public boolean hasAgg() {
    return aggExps != null && !aggExps.isEmpty();
  }

  @Override
  public List<OneDBExpression> getAggExps() {
    return aggExps;
  }

  @Override
  public void setAggExps(List<OneDBExpression> aggExps) {
    if (this.aggExps != null && !this.aggExps.isEmpty()) {
      LOG.error("leaf query does not support aggregate function nesting");
      throw new RuntimeException("leaf query does not support aggregate function nesting");
    }
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
  public List<String> getOrders() {
    return orders;
  }

  @Override
  public void setOrders(List<String> orders) {
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

  public List<FieldType> getSelectTypes() {
    return selectExps.stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public QueryableDataSet implement(OneDBImplementor implementor) {
    OneDBUnaryContext unary = implementor.rewriteLeaf(this);
    QueryableDataSet result = implementor.leafQuery(this);
    if (unary != null) {
      return unary.implementInternal(implementor, result);
    } else {
      return result;
    }
  }
}
