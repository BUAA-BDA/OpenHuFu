package com.hufudb.onedb.core.rewriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.expression.AggFuncType;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.plan.BinaryPlan;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.plan.RootPlan;
import com.hufudb.onedb.plan.UnaryPlan;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import com.hufudb.onedb.rewriter.Rewriter;

public class BasicRewriter implements Rewriter {
  final OneDBClient client;

  public BasicRewriter(OneDBClient client) {
    this.client = client;
  }

  @Override
  public void rewriteChild(Plan context) {
    context.rewrite(this);
  }

  @Override
  public Plan rewriteRoot(RootPlan root) {
    return root;
  }

  @Override
  public Plan rewriteBianry(BinaryPlan binary) {
    return binary;
  }

  @Override
  public Plan rewriteUnary(UnaryPlan unary) {
    return unary;
  }

  @Override
  public Plan rewriteLeaf(LeafPlan leaf) {
    // only horizontal partitioned table need rewrite
    if (client.getTableSchema(leaf.getTableName()).ownerSize() > 1) {
      boolean hasAgg = leaf.hasAgg();
      boolean hasLimit = leaf.getOffset() != 0 || leaf.getFetch() != 0;
      boolean hasSort = leaf.getOrders() != null && !leaf.getOrders().isEmpty();
      if (!hasAgg && !hasLimit && !hasSort) {
        // return leaf directly if no aggergate, limit or sort
        return leaf;
      }
      UnaryPlan unary = new UnaryPlan(leaf);
      if (hasAgg) {
        rewriteAggregations(unary, leaf);
      } else {
        if (hasLimit) {
          unary.setFetch(leaf.getFetch());
          unary.setOffset(leaf.getOffset());
        }
        if (hasSort) {
          unary.setOrders(leaf.getOrders());
        }
        unary.setSelectExps(ExpressionFactory.createInputRef(leaf.getSelectExps()));
      }
      return unary;
    } else {
      return leaf;
    }
  }

  /**
   * for aggregate call with distinct flag, add the inputRefs into local group set and update the
   * global group set
   */
  private Expression updateGroupIdx(Expression agg, List<Expression> localAggs,
      Map<Integer, Expression> groupMap) {
    Expression.Builder builder = agg.toBuilder();
    builder.clearIn();
    List<Expression> inputRefs = agg.getInList();
    for (int i = 0; i < inputRefs.size(); ++i) {
      Expression inputRef = inputRefs.get(i);
      int ref = inputRef.getI32();
      if (!groupMap.containsKey(ref)) {
        int groupKeyIdx = localAggs.size();
        // for distinct agg, the distinct key is not group key in global agg
        Expression groupRef = ExpressionFactory.createInputRef(groupKeyIdx, inputRef.getOutType(),
            inputRef.getModifier());
        groupMap.put(ref, groupRef);
        localAggs.add(ExpressionFactory.createAggFunc(inputRef.getOutType(), inputRef.getModifier(),
            AggFuncType.GROUPKEY.getId(), ImmutableList.of(inputRef)));
        builder.addIn(groupRef);
      } else {
        builder.addIn(groupMap.get(ref));
      }
    }
    return builder.build();
  }

  private Expression convertAvg(Expression agg, List<Expression> localAggs,
      Map<Integer, Expression> groupMap) {
    if (!AggFuncType.isDistinct(agg.getI32())) {
      Expression localAvgSum = ExpressionFactory.createAggFunc(agg.getOutType(), agg.getModifier(),
          AggFuncType.SUM.getId(), agg.getInList());
      int localAvgSumRef = localAggs.size();
      localAggs.add(localAvgSum);
      Expression localAvgSumRefExp =
          ExpressionFactory.createInputRef(localAvgSumRef, agg.getOutType(), agg.getModifier());
      Expression globalAvgSum = ExpressionFactory.createAggFunc(agg.getOutType(), agg.getModifier(),
          AggFuncType.SUM.getId(), ImmutableList.of(localAvgSumRefExp));

      // add a sum layer above count
      Expression localAvgCount = ExpressionFactory.createAggFunc(agg.getOutType(),
          agg.getModifier(), AggFuncType.COUNT.getId(), agg.getInList());
      int localAvgCntRef = localAggs.size();
      localAggs.add(localAvgCount);
      Expression localAvgCntRefExp =
          ExpressionFactory.createInputRef(localAvgCntRef, agg.getOutType(), agg.getModifier());
      Expression globalAvgCnt = ExpressionFactory.createAggFunc(agg.getOutType(), agg.getModifier(),
          AggFuncType.SUM.getId(), ImmutableList.of(localAvgCntRefExp));

      return ExpressionFactory.createBinaryOperator(OperatorType.DIVIDE, agg.getOutType(),
          globalAvgSum, globalAvgCnt);
    } else {
      return updateGroupIdx(agg, localAggs, groupMap);
    }
  }

  private Expression convertCount(Expression agg, List<Expression> localAggs,
      Map<Integer, Expression> groupMap) {
    if (!AggFuncType.isDistinct(agg.getI32())) {
      localAggs.add(agg);
      // add a sum layer above count
      Expression ref = ExpressionFactory.createInputRef(localAggs.size() - 1, agg.getOutType(),
      agg.getModifier());
      return ExpressionFactory.createAggFunc(agg.getOutType(), agg.getModifier(),
      AggFuncType.SUM.getId(), ImmutableList.of(ref));
    } else {
      return updateGroupIdx(agg, localAggs, groupMap);
    }
  }

  private Expression convertSum(Expression agg, List<Expression> localAggs,
      Map<Integer, Expression> groupMap) {
    if (!AggFuncType.isDistinct(agg.getI32())) {
      localAggs.add(agg);
      Expression ref = ExpressionFactory.createInputRef(localAggs.size() - 1, agg.getOutType(),
          agg.getModifier());
      return ExpressionFactory.createAggFunc(agg.getOutType(), agg.getModifier(),
          AggFuncType.SUM.getId(), ImmutableList.of(ref));
    } else {
      return updateGroupIdx(agg, localAggs, groupMap);
    }
  }

  private Expression convertGroupKey(Expression agg, List<Expression> localAggs,
      Map<Integer, Expression> groupMap) {
    int groupRef = agg.getIn(0).getI32();
    if (groupMap.containsKey(groupRef)) {
      return ExpressionFactory.createAggFunc(agg.getOutType(), agg.getModifier(), AggFuncType.GROUPKEY.getId(), ImmutableList.of(groupMap.get(groupRef)));
    } else {
      LOG.error("Group key should be presented in group by clause");
      throw new RuntimeException("Group key should be presented in group by clause");
    }
  }

  // convert agg into two parts: local aggs and global agg, local agg are added into localAggs,
  // global agg is returned
  private Expression convertAgg(Expression agg, List<Expression> localAggs,
      Map<Integer, Expression> groupMap) {
    AggFuncType type = AggFuncType.of(Math.abs(agg.getI32()));
    switch (type) {
      case AVG: // avg converted to global: div(sum(sumref), sum(countref)), local: sum, count
        return convertAvg(agg, localAggs, groupMap);
      case COUNT: // count converted global sum(countref), local: count
        return convertCount(agg, localAggs, groupMap);
      case SUM:
        return convertSum(agg, localAggs, groupMap);
      case GROUPKEY:
        return convertGroupKey(agg, localAggs, groupMap);
      default: // others convert directly
        localAggs.add(agg);
        Expression ref = ExpressionFactory.createInputRef(localAggs.size() - 1, agg.getOutType(),
            agg.getModifier());
        return ExpressionFactory.createAggFunc(agg.getOutType(), agg.getModifier(), type.getId(),
            ImmutableList.of(ref));
    }
  }

  // rewrite exp into global agg and add new local aggs into localAggs
  private Expression rewriteAggregate(Expression exp, List<Expression> localAggs,
      Map<Integer, Expression> groupMap) {
    // traverse exp tree, and convert each aggCall
    if (exp.getOpType().equals(OperatorType.AGG_FUNC)) {
      return convertAgg((Expression) exp, localAggs, groupMap);
    } else if (exp.getInCount() > 0) {
      List<Expression> children = exp.getInList();
      Expression.Builder builder = exp.toBuilder();
      builder.clearIn();
      for (int i = 0; i < children.size(); ++i) {
        Expression globalExp = rewriteAggregate(children.get(i), localAggs, groupMap);
        builder.addIn(globalExp);
      }
      return builder.build();
    } else {
      return exp;
    }
  }

  /*
   * divide aggregation into local part and global part
   */
  private void rewriteAggregations(UnaryPlan unary, LeafPlan leaf) {
    Map<Integer, Expression> groupMap = new TreeMap<>(); // local group ref -> global group ref
                                                         // expression
    List<Expression> originAggs = leaf.getAggExps();
    List<Expression> localAggs = new ArrayList<>();
    List<Expression> globalAggs = new ArrayList<>();
    List<Integer> globalGroups = new ArrayList<>();
    List<Expression> inputs = leaf.getSelectExps();
    // add local groups into local aggs as group key function
    int idx = 0;
    for (int groupRef : leaf.getGroups()) {
      Expression in = inputs.get(groupRef);
      Expression ref =
          ExpressionFactory.createInputRef(groupRef, in.getOutType(), in.getModifier());
      localAggs.add(ExpressionFactory.createAggFunc(ref.getOutType(), ref.getModifier(),
          AggFuncType.GROUPKEY.getId(), ImmutableList.of(ref)));
      groupMap.put(groupRef,
          ExpressionFactory.createInputRef(idx, ref.getOutType(), ref.getModifier()));
      globalGroups.add(idx);
      ++idx;
    }
    // note: keep the rewritten output pattern same as the origin
    for (Expression exp : originAggs) {
      Expression rewrittenExp = rewriteAggregate(exp, localAggs, groupMap);
      globalAggs.add(rewrittenExp);
    }
    unary.setSelectExps(ExpressionFactory.createInputRef(localAggs));
    unary.setAggExps(globalAggs);
    unary.setGroups(globalGroups);
    leaf.setGroups(groupMap.keySet().stream().collect(Collectors.toList()));
    // delete sort and limit operations in leaf node and add them in unary node if exist
    boolean hasLimit = leaf.getOffset() != 0 || leaf.getFetch() != 0;
    boolean hasSort = leaf.getOrders() != null && !leaf.getOrders().isEmpty();
    if (hasLimit) {
      unary.setFetch(leaf.getFetch());
      unary.setOffset(leaf.getOffset());
      leaf.setFetch(0);
      leaf.setOffset(0);
    }
    if (hasSort) {
      unary.setOrders(leaf.getOrders());
      leaf.setOrders(new ArrayList<>());
    }
    leaf.setAggExps(localAggs);
  }
}
