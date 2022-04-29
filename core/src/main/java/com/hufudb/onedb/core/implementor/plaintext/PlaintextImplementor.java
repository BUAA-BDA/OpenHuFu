package com.hufudb.onedb.core.implementor.plaintext;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.StreamBuffer;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBOpType;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall.AggregateType;
import com.hufudb.onedb.core.sql.expression.OneDBOperator.FuncType;
import com.hufudb.onedb.core.sql.context.OneDBContextType;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import com.hufudb.onedb.core.sql.context.OneDBRootContext;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.calcite.util.Pair;

/*
 * plaintext implementor of onedb query proto
 */
public class PlaintextImplementor implements OneDBImplementor {

  private final OneDBClient client;

  public PlaintextImplementor(OneDBClient client) {
    this.client = client;
  }


  public QueryableDataSet implement(OneDBContext context) {
    if (context.getContextType().equals(OneDBContextType.ROOT)) {
      context = ((OneDBRootContext) context).getChild();
    }
    return context.implement(this);
  }

  private StreamBuffer<DataSetProto> tableQuery(OneDBQueryProto query,
      List<Pair<OwnerClient, String>> tableClients) {
    StreamBuffer<DataSetProto> iterator = new StreamBuffer<>(tableClients.size());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (Pair<OwnerClient, String> entry : tableClients) {
      tasks.add(() -> {
        try {
          OneDBQueryProto localQuery = query.toBuilder().setTableName(entry.getValue()).build();
          Iterator<DataSetProto> it = entry.getKey().oneDBQuery(localQuery);
          while (it.hasNext()) {
            iterator.add(it.next());
          }
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        } finally {
          iterator.finish();
        }
      });
    }
    try {
      List<Future<Boolean>> statusList = client.getThreadPool().invokeAll(tasks);
      for (Future<Boolean> status : statusList) {
        if (!status.get()) {
          LOG.error("error in oneDBQuery");
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error in OneDBQuery for {}", e.getMessage());
    }
    return iterator;
  }

  @Override
  public QueryableDataSet join(QueryableDataSet left, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    return PlaintextNestedLoopJoin.apply(left, right, joinInfo);
  }

  @Override
  public QueryableDataSet filter(QueryableDataSet in, List<OneDBExpression> filters) {
    return PlaintextFilter.apply(in, filters);
  }

  @Override
  public QueryableDataSet project(QueryableDataSet in, List<OneDBExpression> projects) {
    return PlaintextCalculator.apply(in, projects);
  }

  @Override
  public QueryableDataSet aggregate(QueryableDataSet in, List<Integer> groups,
      List<OneDBExpression> aggs, List<FieldType> inputTypes) {
    return PlaintextAggregation.apply(in, groups, aggs, inputTypes);
  }

  @Override
  public QueryableDataSet sort(QueryableDataSet in, List<String> orders) {
    return PlaintextSort.apply(in, orders);
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
            FieldType.UNKOWN));
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
          OneDBAggCall.create(AggregateType.SUM, agg.getInputRef(), agg.getOutType());
      int localAvgSumRef = localAggs.size();
      localAggs.add(localAvgSum);
      OneDBAggCall globalAvgSum = OneDBAggCall.create(AggregateType.SUM,
          ImmutableList.of(localAvgSumRef), agg.getOutType());
      // add a sum layer above count
      OneDBAggCall localAvgCount =
          OneDBAggCall.create(AggregateType.COUNT, agg.getInputRef(), agg.getOutType());
      int localAvgCntRef = localAggs.size();
      localAggs.add(localAvgCount);
      OneDBAggCall globalAvgCount = OneDBAggCall.create(AggregateType.SUM,
          ImmutableList.of(localAvgCntRef), agg.getOutType());
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
          agg.getOutType());
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
          agg.getOutType());
    } else {
      updateGroupIdx(agg, localAggs, groupMap);
      return agg;
    }
  }

  private OneDBExpression convertGroupKey(OneDBAggCall agg, List<OneDBExpression> localAggs,
      Map<Integer, Integer> groupMap) {
    if (groupMap.containsKey(agg.getInputRef().get(0))) {
      return OneDBAggCall.create(AggregateType.GROUPKEY,
          ImmutableList.of(groupMap.get(agg.getInputRef().get(0))), agg.getOutType());
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
            agg.getOutType());
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
    List<Integer> globalGroups = new ArrayList();
    List<FieldType> selectTypes = leaf.getSelectTypes();
    // add local groups into local aggs as group key function
    int idx = 0;
    for (int groupRef : leaf.getGroups()) {
      FieldType type = selectTypes.get(groupRef);
      localAggs.add(OneDBAggCall.create(AggregateType.GROUPKEY, ImmutableList.of(groupRef), type));
      groupMap.put(groupRef, idx);
      globalGroups.add(idx);
      ++idx;
    }
    // note: keep the rewritten output pattern same as the origin
    for (OneDBExpression exp : originAggs) {
      OneDBExpression rewrittenExp = rewriteAggregate(exp, localAggs, groupMap);
      globalAggs.add(rewrittenExp);
    }
    unary.setAggExps(globalAggs);
    unary.setGroups(globalGroups);
    leaf.setGroups(groupMap.keySet().stream().collect(Collectors.toList()));
    // delete sort and limit operations in leaf node and add them in unary node if exist
    boolean hasLimit = leaf.getOffset() != 0 || leaf.getFetch() != 0;
    boolean hasSort = leaf.getOrders() != null && !leaf.getOrders().isEmpty();
    if (hasLimit) {
      unary.setFetch(leaf.getFetch());
      unary.setOffset(leaf.getOffset());
      leaf.setOffset(0);
      leaf.setFetch(0);
    }
    if (hasSort) {
      unary.setOrders(leaf.getOrders());
      leaf.setOrders(new ArrayList<>());
    }
    originAggs.clear();
    originAggs.addAll(localAggs);
  }

  public OneDBUnaryContext rewriteLeaf(OneDBLeafContext leaf) {
    boolean hasAgg = leaf.hasAgg();
    boolean hasLimit = leaf.getOffset() != 0 || leaf.getFetch() != 0;
    boolean hasSort = leaf.getOrders() != null && !leaf.getOrders().isEmpty();
    if (!hasAgg && !hasLimit && !hasSort) {
      // return null if no aggergate, limit or sort
      return null;
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
    }
    return unary;
  }

  public QueryableDataSet leafQuery(OneDBLeafContext leaf) {
    OneDBQueryProto proto = leaf.toProto();
    List<Pair<OwnerClient, String>> tableClients = client.getTableClients(leaf.getTableName());
    StreamBuffer<DataSetProto> streamProto = tableQuery(proto, tableClients);

    Header header = OneDBContext.getOutputHeader(proto);
    // todo: optimze for streamDataSet
    BasicDataSet localDataSet = BasicDataSet.of(header);
    while (streamProto.hasNext()) {
      localDataSet.mergeDataSet(BasicDataSet.fromProto(streamProto.next()));
    }
    return PlaintextQueryableDataSet.fromBasic(localDataSet);
  }
}
