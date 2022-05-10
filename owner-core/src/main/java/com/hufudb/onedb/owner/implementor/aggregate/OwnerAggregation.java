package com.hufudb.onedb.owner.implementor.aggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.aggregate.AggregateFunction;
import com.hufudb.onedb.core.implementor.aggregate.Aggregator;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.owner.implementor.OwnerQueryableDataSet;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.OneDBCommon.TaskInfoProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnerAggregation {
  static final Logger LOG = LoggerFactory.getLogger(OwnerAggregation.class);

  public static QueryableDataSet apply(QueryableDataSet input, List<Integer> groups, List<OneDBExpression> aggs, List<FieldType> types, Rpc rpc, ExecutorService threadPool, TaskInfoProto taskInfo) {
    List<AggregateFunction<Row, Comparable>> aggFunctions = new ArrayList<>();
    List<FieldType> aggTypes = new ArrayList<>();
    if (!groups.isEmpty()) {
      LOG.warn("Not support 'group by' clause");
      throw new UnsupportedOperationException("Not support 'group by' clause");
    }
    if (taskInfo.getPartiesList().size() != 2) {
      LOG.warn("Just support 2 parties in aggregation");
      throw new UnsupportedOperationException("Just support 2 parties in aggregation");
    }
    for (OneDBExpression exp : aggs) {
      aggFunctions.add(OwnerAggregteFunctions.getAggregateFunc(exp, rpc, threadPool, taskInfo));
      aggTypes.add(exp.getOutType());
    }
    Aggregator aggregator = Aggregator.create(groups, aggFunctions, aggTypes);
    int receiverId = taskInfo.getParties(1);
    return applyAggregateFunctions(input, aggregator, receiverId == rpc.ownParty().getPartyId());
  }

  public static QueryableDataSet applyAggregateFunctions(QueryableDataSet input,
  Aggregator aggregator, boolean isReporter) {
    // aggregate input rows
    Header.Builder builder = Header.newBuilder();
    aggregator.getOutputTypes().stream().forEach(type -> builder.add("", type));
    QueryableDataSet result = new OwnerQueryableDataSet(builder.build());
    List<Row> rows = input.getRows();
    for (Row row : rows) {
      aggregator.add(row);
    }
    while (aggregator.hasNext()) {
      Row row = aggregator.aggregate();
      if (isReporter) {
        result.addRow(row);
      }
    }
    return result;
  }
}
