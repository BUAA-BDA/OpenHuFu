package com.hufudb.onedb.owner.implementor.aggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import com.hufudb.onedb.data.function.AggregateFunction;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.AggDataSet;
import com.hufudb.onedb.data.storage.ArrayDataSet;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.expression.ExpressionUtils;
import com.hufudb.onedb.expression.SingleAggregator;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.TaskInfo;
import com.hufudb.onedb.rpc.Rpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnerAggregation {
  static final Logger LOG = LoggerFactory.getLogger(OwnerAggregation.class);

  public static DataSet aggregate(DataSet input, List<Integer> groups, List<Expression> aggs, List<ColumnType> types, Rpc rpc, ExecutorService threadPool, TaskInfo taskInfo) {
    List<AggregateFunction<Row, Comparable>> aggFunctions = new ArrayList<>();
    List<ColumnType> aggTypes = new ArrayList<>();
    if (!groups.isEmpty()) {
      LOG.warn("Not support 'group by' clause");
      throw new UnsupportedOperationException("Not support 'group by' clause");
    }
    if (taskInfo.getPartiesList().size() != 2) {
      LOG.warn("Just support 2 parties in aggregation");
      throw new UnsupportedOperationException("Just support 2 parties in aggregation");
    }
    for (Expression exp : aggs) {
      aggFunctions.add(OwnerAggregteFunctions.getAggregateFunc(exp, rpc, threadPool, taskInfo));
      aggTypes.add(exp.getOutType());
    }
    // Aggregator aggregator = Aggregator.create(groups, aggFunctions, aggTypes);
    Schema outSchema = ExpressionUtils.createSchema(aggs);
    DataSet result = ArrayDataSet.materialize(AggDataSet.create(outSchema, new SingleAggregator(outSchema, aggFunctions), input));
    if (taskInfo.getParties(1) == rpc.ownParty().getPartyId()) {
      return result;
    } else {
      return EmptyDataSet.INSTANCE;
    }
  }
}
