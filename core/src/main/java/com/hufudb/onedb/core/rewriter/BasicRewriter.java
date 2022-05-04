package com.hufudb.onedb.core.rewriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.sql.context.OneDBBinaryContext;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBRootContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBOpType;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import com.hufudb.onedb.core.sql.expression.OneDBReference;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall.AggregateType;
import com.hufudb.onedb.core.sql.expression.OneDBOperator.FuncType;

public class BasicRewriter implements OneDBRewriter {

  @Override
  public void rewriteChild(OneDBContext context) {
    context.rewrite(this);
  }

  @Override
  public OneDBContext rewriteRoot(OneDBRootContext root) {
    return root;
  }

  @Override
  public OneDBContext rewriteBianry(OneDBBinaryContext binary) {
    return binary;
  }

  @Override
  public OneDBContext rewriteUnary(OneDBUnaryContext unary) {
    return unary;
  }

  @Override
  public OneDBContext rewriteLeaf(OneDBLeafContext leaf) {
    // only horizontal partitioned table need rewrite
    if (leaf.ownerSize() > 1) {
      boolean hasAgg = leaf.hasAgg();
      boolean hasLimit = leaf.getOffset() != 0 || leaf.getFetch() != 0;
      boolean hasSort = leaf.getOrders() != null && !leaf.getOrders().isEmpty();
      if (!hasAgg && !hasLimit && !hasSort) {
        // return leaf directly if no aggergate, limit or sort
        return leaf;
      }
      OneDBUnaryContext unary = new OneDBUnaryContext();
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
        unary.setSelectExps(OneDBReference.fromExps(leaf.getSelectExps()));
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
  private void updateGroupIdx(OneDBAggCall agg, List<OneDBExpression> localAggs,
      Map<Integer, Integer> groupMap) {
    List<Integer> inputRefs = agg.getInputRef();
    for (int i = 0; i < inputRefs.size(); ++i) {
      int inputRef = inputRefs.get(i);
      if (!groupMap.containsKey(inputRef)) {
        int groupKeyIdx = localAggs.size();
        // for distinct agg, the distinct key is not group key in global agg
        groupMap.put(inputRef, groupKeyIdx);
        localAggs.add(OneDBAggCall.create(AggregateType.GROUPKEY, ImmutableList.of(inputRef),
            FieldType.UNKOWN, Level.PUBLIC));
        inputRefs.set(i, groupKeyIdx);
      } else {
        inputRefs.set(i, groupMap.get(inputRef));
      }
    }
  }

  private OneDBExpression convertAvg(OneDBAggCall agg, List<OneDBExpression> localAggs,
      Map<Integer, Integer> groupMap) {
    if (!agg.isDistinct()) {
      OneDBAggCall localAvgSum =
          OneDBAggCall.create(AggregateType.SUM, agg.getInputRef(), agg.getOutType(), Level.PUBLIC);
      int localAvgSumRef = localAggs.size();
      localAggs.add(localAvgSum);
      OneDBAggCall globalAvgSum = OneDBAggCall.create(AggregateType.SUM,
          ImmutableList.of(localAvgSumRef), agg.getOutType(), Level.PUBLIC);
      // add a sum layer above count
      OneDBAggCall localAvgCount =
          OneDBAggCall.create(AggregateType.COUNT, agg.getInputRef(), agg.getOutType(), Level.PUBLIC);
      int localAvgCntRef = localAggs.size();
      localAggs.add(localAvgCount);
      OneDBAggCall globalAvgCount = OneDBAggCall.create(AggregateType.SUM,
          ImmutableList.of(localAvgCntRef), agg.getOutType(), Level.PUBLIC);
      return OneDBOperator.create(OneDBOpType.DIVIDE, agg.getOutType(),
          new ArrayList<>(Arrays.asList(globalAvgSum, globalAvgCount)), FuncType.NONE);
    } else {
      updateGroupIdx(agg, localAggs, groupMap);
      return agg;
    }
  }

  private OneDBExpression convertCount(OneDBAggCall agg, List<OneDBExpression> localAggs,
      Map<Integer, Integer> groupMap) {
    if (!agg.isDistinct()) {
      localAggs.add(agg);
      // add a sum layer above count
      return OneDBAggCall.create(AggregateType.SUM, ImmutableList.of(localAggs.size() - 1),
          agg.getOutType(), Level.PUBLIC);
    } else {
      updateGroupIdx(agg, localAggs, groupMap);
      return agg;
    }
  }

  private OneDBExpression convertSum(OneDBAggCall agg, List<OneDBExpression> localAggs,
      Map<Integer, Integer> groupMap) {
    if (!agg.isDistinct()) {
      localAggs.add(agg);
      return OneDBAggCall.create(AggregateType.SUM, ImmutableList.of(localAggs.size() - 1),
          agg.getOutType(), Level.PUBLIC);
    } else {
      updateGroupIdx(agg, localAggs, groupMap);
      return agg;
    }
  }

  private OneDBExpression convertGroupKey(OneDBAggCall agg, List<OneDBExpression> localAggs,
      Map<Integer, Integer> groupMap) {
    if (groupMap.containsKey(agg.getInputRef().get(0))) {
      return OneDBAggCall.create(AggregateType.GROUPKEY,
          ImmutableList.of(groupMap.get(agg.getInputRef().get(0))), agg.getOutType(), Level.PUBLIC);
    } else {
      LOG.error("Group key should be presented in group by clause");
      throw new RuntimeException("Group key should be presented in group by clause");
    }
  }

  // convert agg into two parts: local aggs and global agg, local agg are added into localAggs,
  // global agg is returned
  private OneDBExpression convertAgg(OneDBAggCall agg, List<OneDBExpression> localAggs,
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
            agg.getOutType(), Level.PUBLIC);
    }
  }

  // rewrite exp into global agg and add new local aggs into localAggs
  private OneDBExpression rewriteAggregate(OneDBExpression exp, List<OneDBExpression> localAggs,
      Map<Integer, Integer> groupMap) {
    // traverse exp tree, and convert each aggCall
    if (exp instanceof OneDBAggCall) {
      return convertAgg((OneDBAggCall) exp, localAggs, groupMap);
    } else if (exp instanceof OneDBOperator) {
      List<OneDBExpression> children = ((OneDBOperator) exp).getInputs();
      for (int i = 0; i < children.size(); ++i) {
        OneDBExpression globalExp = rewriteAggregate(children.get(i), localAggs, groupMap);
        children.set(i, globalExp);
      }
    }
    return exp;
  }

  /*
   * divide aggregation into local part and global part
   */
  private void rewriteAggregations(OneDBUnaryContext unary, OneDBLeafContext leaf) {
    Map<Integer, Integer> groupMap = new TreeMap<>(); // local group ref -> global group ref
    List<OneDBExpression> originAggs = leaf.getAggExps();
    List<OneDBExpression> localAggs = new ArrayList<>();
    List<OneDBExpression> globalAggs = new ArrayList<>();
    List<Integer> globalGroups = new ArrayList<>();
    List<FieldType> selectTypes = leaf.getSelectTypes();
    // add local groups into local aggs as group key function
    int idx = 0;
    for (int groupRef : leaf.getGroups()) {
      FieldType type = selectTypes.get(groupRef);
      localAggs.add(OneDBAggCall.create(AggregateType.GROUPKEY, ImmutableList.of(groupRef), type, Level.PUBLIC));
      groupMap.put(groupRef, idx);
      globalGroups.add(idx);
      ++idx;
    }
    // note: keep the rewritten output pattern same as the origin
    for (OneDBExpression exp : originAggs) {
      OneDBExpression rewrittenExp = rewriteAggregate(exp, localAggs, groupMap);
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
