package com.hufudb.openhufu.owner.implementor.aggregate;

import com.hufudb.openhufu.expression.AggFuncType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.hufudb.openhufu.data.function.AggregateFunction;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.AggDataSet;
import com.hufudb.openhufu.data.storage.ArrayDataSet;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.EmptyDataSet;
import com.hufudb.openhufu.data.storage.Row;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.expression.ExpressionUtils;
import com.hufudb.openhufu.expression.SingleAggregator;
import com.hufudb.openhufu.interpreter.Interpreter;
import com.hufudb.openhufu.owner.implementor.OwnerImplementorFactory;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.TaskInfo;
import com.hufudb.openhufu.rpc.Rpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnerAggregation {
  static final Logger LOG = LoggerFactory.getLogger(OwnerAggregation.class);

  public static AggregateFunction getAggregateFunc(Expression exp, Rpc rpc, ExecutorService threadPool, TaskInfo taskInfo) {
    if (exp.getOpType().equals(OpenHuFuPlan.OperatorType.AGG_FUNC)) {
      LOG.info("using aggfunc: " + AggFuncType.of(exp.getI32()).getName());
      return OwnerImplementorFactory.getAggregationFunction(AggFuncType.of(exp.getI32()), exp, rpc, threadPool, taskInfo);
    } else {
      throw new UnsupportedOperationException("Just support single aggregate function");
    }
  }

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
        aggFunctions.add(getAggregateFunc(exp, rpc, threadPool, taskInfo));
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
          aggFunctions.add(getAggregateFunc(inner, rpc, threadPool, taskInfo));
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
