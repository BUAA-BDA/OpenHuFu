package com.hufudb.onedb.core.implementor.plaintext;

import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.StreamBuffer;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.UserSideImplementor;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.calcite.util.Pair;

/*
 * plaintext implementor of onedb query proto
 */
public class PlaintextImplementor extends UserSideImplementor {

  private final OneDBClient client;

  public PlaintextImplementor(OneDBClient client) {
    this.client = client;
  }

  private StreamBuffer<DataSetProto> tableQuery(QueryContextProto query,
      List<Pair<OwnerClient, String>> tableClients) {
    StreamBuffer<DataSetProto> iterator = new StreamBuffer<>(tableClients.size());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (Pair<OwnerClient, String> entry : tableClients) {
      tasks.add(() -> {
        try {
          QueryContextProto localQuery = query.toBuilder().setTableName(entry.getValue()).build();
          Iterator<DataSetProto> it = entry.getKey().query(localQuery);
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
          LOG.error("error in leafQuery");
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error in leafQuery for {}", e.getMessage());
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
  public QueryableDataSet sort(QueryableDataSet in, List<OneDBOrder> orders) {
    return PlaintextSort.apply(in, orders);
  }

  public QueryableDataSet leafQuery(OneDBLeafContext leaf) {
    QueryContextProto proto = leaf.toProto();
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
