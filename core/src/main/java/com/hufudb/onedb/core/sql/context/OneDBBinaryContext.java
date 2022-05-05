package com.hufudb.onedb.core.sql.context;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.rewriter.OneDBRewriter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;
import com.hufudb.onedb.rpc.OneDBCommon.TaskInfoProto;
import org.apache.commons.lang3.tuple.Pair;

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
    QueryableDataSet leftResult = implementor.implement(left);
    QueryableDataSet rightResult = implementor.implement(right);
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

  @Override
  public List<Pair<OwnerClient, QueryContextProto>> generateOwnerContextProto(OneDBClient client) {
    QueryContextProto.Builder contextBuilder = QueryContextProto.newBuilder()
        .setContextType(OneDBContextType.UNARY.ordinal()).setFetch(fetch).setOffset(offset);
    if (selectExps != null) {
      contextBuilder.addAllSelectExp(OneDBExpression.toProto(selectExps));
    }
    if (aggExps != null) {
      contextBuilder.addAllAggExp(OneDBExpression.toProto(aggExps));
    }
    if (groups != null) {
      contextBuilder.addAllGroup(groups);
    }
    if (orders != null) {
      contextBuilder.addAllOrder(OneDBOrder.toProto(orders));
    }
    List<Pair<OwnerClient, QueryContextProto>> leftContext = left.generateOwnerContextProto(client);
    List<Pair<OwnerClient, QueryContextProto>> rightContext =
        right.generateOwnerContextProto(client);
    List<Pair<OwnerClient, QueryContextProto>> ownerContext = new ArrayList<>();
    // contextBuilder.setJoinInfo(joinInfo.toProto());
    TaskInfoProto.Builder taskInfo = TaskInfoProto.newBuilder().setTaskId(client.getTaskId());
    for (Pair<OwnerClient, QueryContextProto> p : leftContext) {
      taskInfo.addParties(p.getLeft().getParty().getPartyId());
    }
    for (Pair<OwnerClient, QueryContextProto> p : rightContext) {
      taskInfo.addParties(p.getLeft().getParty().getPartyId());
    }
    contextBuilder.setTaskInfo(taskInfo);

    // for owners from left
    contextBuilder.setJoinInfo(joinInfo.toProto(true));
    for (Pair<OwnerClient, QueryContextProto> p : leftContext) {
      QueryContextProto context = contextBuilder.addChildren(0, p.getValue())
          .addChildren(1, OneDBPlaceholderContext.PLACEHOLDER_PROTO).build();
      p.setValue(context);
      ownerContext.add(p);
    }
    // for owners from right
    contextBuilder.setJoinInfo(joinInfo.toProto(false));
    for (Pair<OwnerClient, QueryContextProto> p : rightContext) {
      QueryContextProto context = contextBuilder.setChildren(0, OneDBPlaceholderContext.PLACEHOLDER_PROTO).setChildren(1, p.getValue()).build();
      p.setValue(context);
      ownerContext.add(p);
    }
    return ownerContext;
  }
}
