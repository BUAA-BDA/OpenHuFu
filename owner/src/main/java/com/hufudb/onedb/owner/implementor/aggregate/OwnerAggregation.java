package com.hufudb.onedb.owner.implementor.aggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.function.AggregateFunction;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.AggDataSet;
import com.hufudb.onedb.data.storage.ArrayDataSet;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.expression.ExpressionUtils;
import com.hufudb.onedb.expression.SingleAggregator;
import com.hufudb.onedb.interpreter.Interpreter;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBPlan;
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
    // interval aggregations
    List<Expression> interAggs = new ArrayList<>();
    // select from interval aggregations
    List<Expression> finalSelects = new ArrayList<>();
    if (!groups.isEmpty()) {
      LOG.warn("Not support 'group by' clause");
      throw new UnsupportedOperationException("Not support 'group by' clause");
    }
    for (Expression exp : aggs) {
      if (exp.getInCount() == 1) {
        int ref = interAggs.size();
        aggFunctions.add(OwnerAggregateFunctions.getAggregateFunc(exp, rpc, threadPool, taskInfo));
        aggTypes.add(exp.getOutType());
        interAggs.add(exp);
        finalSelects.add(ExpressionFactory.createInputRef(ref, exp.getOutType(), exp.getModifier()));
      } else {
        Expression.Builder builder = exp.toBuilder();
        builder.clearIn();
        // todo: now we only handles one layer like DIVIDE(SUM(), SUM()),
        //  what if there is DIVIDE(DIVIDE(SUM(), SUM()), 3)?
        for (Expression inner : exp.getInList()) {
          int ref = interAggs.size();
          aggFunctions.add(OwnerAggregateFunctions.getAggregateFunc(inner, rpc, threadPool, taskInfo));
          aggTypes.add(exp.getOutType());

          Expression inputRef = ExpressionFactory.createInputRef(ref, inner.getOutType(), inner.getModifier());
          builder.addIn(inputRef);

          interAggs.add(inner);
        }
        finalSelects.add(builder.build());
      }
    }

    Schema outSchema = ExpressionUtils.createSchema(interAggs);
    DataSet result = ArrayDataSet.materialize(AggDataSet.create(outSchema, new SingleAggregator(outSchema, aggFunctions), input));

    // reorder the output
    result = Interpreter.map(result, finalSelects);

    if (taskInfo.getParties(0) == rpc.ownParty().getPartyId()) {
      return result;
    } else {
      return EmptyDataSet.INSTANCE;
    }
  }
}
