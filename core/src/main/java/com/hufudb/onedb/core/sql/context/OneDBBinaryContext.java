package com.hufudb.onedb.core.sql.context;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.rewriter.OneDBRewriter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import com.hufudb.onedb.rpc.OneDBCommon.BinaryQueryProto;

/*
 * context for join
 */
public class OneDBBinaryContext extends OneDBBaseContext {
  OneDBContext parent;
  OneDBContext left;
  OneDBContext right;
  List<OneDBExpression> selectExps;
  List<OneDBExpression> whereExps;
  List<OneDBExpression> aggExps;
  List<Integer> groups;
  List<OneDBOrder> orders;
  int fetch;
  int offset;
  OneDBJoinInfo joinInfo;

  public OneDBBinaryContext(OneDBContext parent, OneDBContext left, OneDBContext right) {
    this.parent = parent;
    this.left = left;
    this.right = right;
  }

  public BinaryQueryProto toProto() {
    BinaryQueryProto.Builder builder = BinaryQueryProto.newBuilder();
    if(selectExps != null) {
      builder.addAllSelectExp(OneDBExpression.toProto(selectExps));
    }
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
    builder.setFetch(fetch).setOffset(fetch).setJoinInfo(joinInfo.toProto());
    return builder.build();
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
  public void setChildren(List<OneDBContext> children) {
    assert children.size() == 2;
    left = children.get(0);
    right = children.get(1);
  }

  @Override
  public List<OneDBContext> getChildren() {
    return ImmutableList.of(left, right);
  }

  @Override
  public void updateChild(OneDBContext newChild, OneDBContext oldChild) {
    if (oldChild == left) {
      left = newChild;
    } else if (oldChild == right) {
      right = newChild;
    } else {
      LOG.error("fail to update child for binary context");
      throw new RuntimeException("fail to update child for binary context");
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
  public List<OneDBExpression> getAggExps() {
    return aggExps;
  }

  @Override
  public void setAggExps(List<OneDBExpression> aggExps) {
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
  public List<FieldType> getOutTypes() {
    return getOutExpressions().stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public Level getContextLevel() {
    return Level.dominate(Level.findDominator(getOutExpressions()), joinInfo.getLevel());
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
      LOG.error("Binary context without output expression");
      throw new RuntimeException("Binary context without output expression");
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
  public OneDBJoinInfo getJoinInfo() {
    return joinInfo;
  }

  @Override
  public void setJoinInfo(OneDBJoinInfo joinInfo) {
    this.joinInfo = joinInfo;
  }

  @Override
  public OneDBContextType getContextType() {
    return OneDBContextType.BINARY;
  }

  @Override
  public QueryableDataSet implement(OneDBImplementor implementor) {
    QueryableDataSet leftResult = left.implement(implementor);
    QueryableDataSet rightResult = right.implement(implementor);
    QueryableDataSet result = leftResult.join(implementor, rightResult, joinInfo);
    if (whereExps != null && !whereExps.isEmpty()) {
      result = result.filter(implementor, whereExps);
    }
    if (selectExps != null && !selectExps.isEmpty()) {
      result = result.project(implementor, selectExps);
    }
    if (aggExps != null && !aggExps.isEmpty()) {
      List<FieldType> types = new ArrayList<>();
      types.addAll(left.getOutTypes());
      types.addAll(right.getOutTypes());
      result = result.aggregate(implementor, groups, aggExps, types);
    }
    if (orders != null && !orders.isEmpty()) {
      result = result.sort(implementor, orders);
    }
    if (fetch > 0 || offset > 0) {
      result = result.limit(offset, fetch);
    }
    return result;
  }

  @Override
  public OneDBContext rewrite(OneDBRewriter rewriter) {
    this.left = left.rewrite(rewriter);
    this.right = right.rewrite(rewriter);
    return rewriter.rewriteBianry(this);
  }
}
