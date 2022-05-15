package com.hufudb.onedb.core.rewriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.plan.BinaryPlan;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.plan.RootPlan;
import com.hufudb.onedb.plan.UnaryPlan;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
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
    if (client.getTable(leaf.getTableName()).ownerSize() > 1) {
      boolean hasAgg = leaf.hasAgg();
      boolean hasLimit = leaf.getOffset() != 0 || leaf.getFetch() != 0;
      boolean hasSort = leaf.getOrders() != null && !leaf.getOrders().isEmpty();
      if (!hasAgg && !hasLimit && !hasSort) {
        // return leaf directly if no aggergate, limit or sort
        return leaf;
      }
      UnaryPlan unary = new UnaryPlan();
      unary.setChildren(ImmutableList.of(leaf));
      unary.setParent(leaf.getParent());
      leaf.setParent(unary);
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

    /*
   * for aggregate call with distinct flag, add the inputRefs into local group set and update the
   * global group set
   */
  private void updateGroupIdx(Expression agg, List<Expression> localAggs,
      Map<Integer, Integer> groupMap) {
    List<Integer> inputRefs = agg.getInputRef();
    for (int i = 0; i < inputRefs.size(); ++i) {
      int inputRef = inputRefs.get(i);
      if (!groupMap.containsKey(inputRef)) {
        int groupKeyIdx = localAggs.size();
        // for distinct agg, the distinct key is not group key in global agg
        groupMap.put(inputRef, groupKeyIdx);
        localAggs.add(OneDBAggCall.create(AggregateType.GROUPKEY, ImmutableList.of(inputRef),
            ColumnType.UNKOWN, agg.getModifier()));
        inputRefs.set(i, groupKeyIdx);
      } else {
        inputRefs.set(i, groupMap.get(inputRef));
      }
    }
  }

  private Expression convertAvg(OneDBAggCall agg, List<Expression> localAggs,
      Map<Integer, Integer> groupMap) {
    if (!agg.isDistinct()) {
      OneDBAggCall localAvgSum =
          OneDBAggCall.create(AggregateType.SUM, agg.getInputRef(), agg.getOutType(), agg.getModifier());
      int localAvgSumRef = localAggs.size();
      localAggs.add(localAvgSum);
      OneDBAggCall globalAvgSum = OneDBAggCall.create(AggregateType.SUM,
          ImmutableList.of(localAvgSumRef), agg.getOutType(), agg.getModifier());
      // add a sum layer above count
      OneDBAggCall localAvgCount =
          OneDBAggCall.create(AggregateType.COUNT, agg.getInputRef(), agg.getOutType(), agg.getModifier());
      int localAvgCntRef = localAggs.size();
      localAggs.add(localAvgCount);
      OneDBAggCall globalAvgCount = OneDBAggCall.create(AggregateType.SUM,
          ImmutableList.of(localAvgCntRef), agg.getOutType(), agg.getModifier());
      return OneDBOperator.create(OneDBOpType.DIVIDE, agg.getOutType(),
          new ArrayList<>(Arrays.asList(globalAvgSum, globalAvgCount)), FuncType.NONE);
    } else {
      updateGroupIdx(agg, localAggs, groupMap);
      return agg;
    }
  }

  private Expression convertCount(OneDBAggCall agg, List<Expression> localAggs,
      Map<Integer, Integer> groupMap) {
    if (!agg.isDistinct()) {
      localAggs.add(agg);
      // add a sum layer above count
      return OneDBAggCall.create(AggregateType.SUM, ImmutableList.of(localAggs.size() - 1),
          agg.getOutType(), agg.getModifier());
    } else {
      updateGroupIdx(agg, localAggs, groupMap);
      return agg;
    }
  }

  private Expression convertSum(OneDBAggCall agg, List<Expression> localAggs,
      Map<Integer, Integer> groupMap) {
    if (!agg.isDistinct()) {
      localAggs.add(agg);
      return OneDBAggCall.create(AggregateType.SUM, ImmutableList.of(localAggs.size() - 1),
          agg.getOutType(), agg.getModifier());
    } else {
      updateGroupIdx(agg, localAggs, groupMap);
      return agg;
    }
  }

  private Expression convertGroupKey(OneDBAggCall agg, List<Expression> localAggs,
      Map<Integer, Integer> groupMap) {
    if (groupMap.containsKey(agg.getInputRef().get(0))) {
      return OneDBAggCall.create(AggregateType.GROUPKEY,
          ImmutableList.of(groupMap.get(agg.getInputRef().get(0))), agg.getOutType(), agg.getModifier());
    } else {
      LOG.error("Group key should be presented in group by clause");
      throw new RuntimeException("Group key should be presented in group by clause");
    }
  }

  // convert agg into two parts: local aggs and global agg, local agg are added into localAggs,
  // global agg is returned
  private Expression convertAgg(OneDBAggCall agg, List<Expression> localAggs,
      Map<Integer, Integer> groupMap) {
    switch (agg.getAggType()) {
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
        return OneDBAggCall.create(agg.getAggType(), ImmutableList.of(localAggs.size() - 1),
            agg.getOutType(), agg.getModifier());
    }
  }

  // rewrite exp into global agg and add new local aggs into localAggs
  private Expression rewriteAggregate(Expression exp, List<Expression> localAggs,
      Map<Integer, Integer> groupMap) {
    // traverse exp tree, and convert each aggCall
    if (exp instanceof OneDBAggCall) {
      return convertAgg((OneDBAggCall) exp, localAggs, groupMap);
    } else if (exp instanceof OneDBOperator) {
      List<Expression> children = ((OneDBOperator) exp).getInputs();
      for (int i = 0; i < children.size(); ++i) {
        Expression globalExp = rewriteAggregate(children.get(i), localAggs, groupMap);
        children.set(i, globalExp);
      }
    }
    return exp;
  }

  /*
   * divide aggregation into local part and global part
   */
  private void rewriteAggregations(UnaryPlan unary, LeafPlan leaf) {
    Map<Integer, Integer> groupMap = new TreeMap<>(); // local group ref -> global group ref
    List<Expression> originAggs = leaf.getAggExps();
    List<Expression> localAggs = new ArrayList<>();
    List<Expression> globalAggs = new ArrayList<>();
    List<Integer> globalGroups = new ArrayList<>();
    List<ColumnType> selectTypes = leaf.getSelectTypes();
    List<Modifier> selectModifiers = leaf.getSelectExps().stream().map(exp -> exp.getModifier()).collect(Collectors.toList());
    // add local groups into local aggs as group key function
    int idx = 0;
    for (int groupRef : leaf.getGroups()) {
      ColumnType type = selectTypes.get(groupRef);
      Modifier level = selectModifiers.get(groupRef);
      localAggs.add(OneDBAggCall.create(AggregateType.GROUPKEY, ImmutableList.of(groupRef), type, level));
      groupMap.put(groupRef, idx);
      globalGroups.add(idx);
      ++idx;
    }
    // note: keep the rewritten output pattern same as the origin
    for (Expression exp : originAggs) {
      Expression rewrittenExp = rewriteAggregate(exp, localAggs, groupMap);
      globalAggs.add(rewrittenExp);
    }
    unary.setSelectExps(OneDBReference.fromExps(localAggs));
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
    originAggs.clear();
    originAggs.addAll(localAggs);
  }
}
