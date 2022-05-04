package com.hufudb.onedb.core.sql.context;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.rewriter.OneDBRewriter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;

/*
 * context for single global table query (horizontal partitioned table)
 */
public class OneDBLeafContext extends OneDBBaseContext {
  OneDBContext parent;
  OneDBTableInfo info;
  List<OneDBExpression> selectExps = new ArrayList<>();
  List<OneDBExpression> whereExps = new ArrayList<>();
  List<OneDBExpression> aggExps = new ArrayList<>();
  List<Integer> groups = new ArrayList<>();
  List<OneDBOrder> orders = new ArrayList<>();
  int fetch;
  int offset;

  public OneDBLeafContext() {
    super();
  }

  public QueryContextProto toProto() {
    QueryContextProto.Builder builder = QueryContextProto.newBuilder();
    builder.setContextType(OneDBContextType.LEAF.ordinal()).setTableName(info.getName())
        .addAllSelectExp(OneDBExpression.toProto(selectExps)).setFetch(fetch).setOffset(offset);
    // there is no task info for leaf query
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
      builder.addAllOrder(OneDBOrder.toProto(orders));
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
    return info.getName();
  }

  @Override
  public void setTableInfo(OneDBTableInfo info) {
    this.info = info;
  }

  @Override
  public List<FieldType> getOutTypes() {
    return getOutExpressions().stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public Level getContextLevel() {
    return Level.findDominator(getOutExpressions());
  }

  @Override
  public List<Level> getOutLevels() {
    return getOutExpressions().stream().map(exp -> exp.getLevel()).collect(Collectors.toList());
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
  public List<OneDBOrder> getOrders() {
    return orders;
  }

  @Override
  public void setOrders(List<OneDBOrder> orders) {
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

  @Override
  public Set<OwnerClient> getOwners() {
    return info.getOwners();
  }

  public List<FieldType> getSelectTypes() {
    return selectExps.stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  public int ownerSize() {
    return info.ownerSize();
  }

  @Override
  public QueryableDataSet implement(OneDBImplementor implementor) {
    return implementor.leafQuery(this);
  }

  @Override
  public OneDBContext rewrite(OneDBRewriter rewriter) {
    return rewriter.rewriteLeaf(this);
  }
}
