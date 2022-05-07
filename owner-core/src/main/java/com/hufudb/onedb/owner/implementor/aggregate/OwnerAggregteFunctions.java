package com.hufudb.onedb.owner.implementor.aggregate;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.aggregate.AggregateFunction;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

public class OwnerAggregteFunctions {
  public static AggregateFunction getAggregateFunc(OneDBExpression exp) {
    if (exp instanceof OneDBAggCall) {
      switch (((OneDBAggCall) exp).getAggType()) {
        case SUM:
          if (exp.getOutType().equals(FieldType.INT)) {
            return new GMWSum((OneDBAggCall) exp);
          }
        default:
          throw new UnsupportedOperationException("Unsupported aggregate function");
      }
    } else {
      throw new UnsupportedOperationException("Just support single aggregate function");
    }
  }

  public static class GMWSum implements AggregateFunction<Row, Comparable> {
    int sum;
    final int inputRef;

    GMWSum(int inputRef) {
      this.sum = 0;
      this.inputRef = inputRef;
    }

    GMWSum(OneDBAggCall agg) {
      this(agg.getInputRef().get(0));
    }

    @Override
    public void add(Row ele) {
      Object e = ele.getObject(inputRef);
      sum += ((Number) e).intValue();
    }

    @Override
    public Comparable aggregate() {
      return sum;
    }

    @Override
    public AggregateFunction<Row, Comparable> patternCopy() {
      return new GMWSum(inputRef);
    }
  }
}
