package com.hufudb.onedb.core.data.query.aggregate;

import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.Row.RowBuilder;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.ExpressionInterpreter;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class PlaintextAggregation {
  public static QueryableDataSet apply(QueryableDataSet input, List<OneDBExpression> aggs) {
    List<Row> rows = input.getRows();
    int length = aggs.size();
    // build aggregate function list
    List<AggregateFunction<Comparable>> aggFunctions = new ArrayList<>();
    for (OneDBExpression agg : aggs) {
      aggFunctions.add(getAggregateFunc((OneDBAggCall) agg));
    }
    // aggregate input rows
    for (Row row : rows) {
      for (int i = 0; i < length; ++i) {
        if (aggFunctions.get(i) instanceof PlaintextCount) {
          aggFunctions.get(i).add(0);
        } else {
          int inputRef = ((OneDBAggCall) aggs.get(i)).getInputRef().get(0);
          aggFunctions.get(i).add((Comparable) row.getObject(inputRef));
        }
      }
    }
    // get result
    RowBuilder builder = Row.newBuilder(length);
    for (int i = 0; i < length; ++i) {
      builder.set(i, ExpressionInterpreter.cast(aggFunctions.get(i).aggregate(), aggs.get(i).getOutType()));
    }
    rows.clear();
    rows.add(builder.build());
    return input;
  }

  static AggregateFunction getAggregateFunc(OneDBAggCall exp) {
    switch(exp.getAggType()) {
      case COUNT:
        return new PlaintextCount();
      case SUM:
        return new PlaintextSum();
      case AVG:
        return new PlaintextAverage();
      case MAX:
        return new PlaintextMax();
      case MIN:
        return new PlaintextMin();
      default:
        throw new UnsupportedOperationException("Unsupport aggregation function");
    }
  }
  static class PlaintextSum implements AggregateFunction<Comparable> {
    BigDecimal sum = BigDecimal.valueOf(0);

    @Override
    public void add(Comparable ele) {
      sum = sum.add(number(ele));
    }

    @Override
    public Comparable aggregate() {
      return sum;
    }
  }

  static class PlaintextCount implements AggregateFunction<Comparable> {
    long count = 0;

    @Override
    public void add(Comparable ele) {
      count++;
    }

    @Override
    public Comparable aggregate() {
      return count;
    }
  }

  static class PlaintextAverage implements AggregateFunction<Comparable> {
    long count = 0;
    BigDecimal sum = BigDecimal.valueOf(0);

    @Override
    public void add(Comparable ele) {
      sum = sum.add(number(ele));
      count++;
    }

    @Override
    public Comparable aggregate() {
      if (count == 0) {
        return 0;
      }
      return sum.divide(BigDecimal.valueOf(count));
    }
  }

  static class PlaintextMax implements AggregateFunction<Comparable> {
    static Comparable MIN = new Comparable() {
      @Override
      public int compareTo(Object o) {
        return -1;
      }
    };
    Comparable maxValue = MIN;

    @Override
    public void add(Comparable ele) {
      if (maxValue.compareTo(ele) < 0) {
        maxValue = ele;
      }
    }

    @Override
    public Comparable aggregate() {
      // todo: consider no value condition
      return maxValue;
    }
  }

  static class PlaintextMin implements AggregateFunction<Comparable> {
    static Comparable MAX = new Comparable() {
      @Override
      public int compareTo(Object o) {
        return 1;
      }
    };
    Comparable minValue = MAX;

    @Override
    public void add(Comparable ele) {
      if (minValue.compareTo(ele) > 0) {
        minValue = ele;
      }
    }

    @Override
    public Comparable aggregate() {
      // todo: consider no value condition
      return minValue;
    }
  }

  private static BigDecimal number(Comparable comparable) {
    return comparable instanceof BigDecimal
        ? (BigDecimal) comparable
        : comparable instanceof BigInteger
            ? new BigDecimal((BigInteger) comparable)
            : comparable instanceof Long
                || comparable instanceof Integer
                || comparable instanceof Short
                    ? new BigDecimal(((Number) comparable).longValue())
                    : new BigDecimal(((Number) comparable).doubleValue());
  }
}
