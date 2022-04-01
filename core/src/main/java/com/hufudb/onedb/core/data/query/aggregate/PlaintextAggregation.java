package com.hufudb.onedb.core.data.query.aggregate;

import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.Row.RowBuilder;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.ExpressionInterpreter;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import com.hufudb.onedb.core.sql.expression.OneDBReference;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class PlaintextAggregation {
  public static QueryableDataSet apply(QueryableDataSet input, List<OneDBExpression> aggs) {
    List<Row> rows = input.getRows();
    int length = aggs.size();
    // build aggregate function list
    List<AggregateFunction<Row, Comparable>> aggFunctions = new ArrayList<>();
    for (OneDBExpression exp : aggs) {
      aggFunctions.add(getAggregateFunc(exp));
    }
    // aggregate input rows
    for (Row row : rows) {
      for (int i = 0; i < length; ++i) {
        aggFunctions.get(i).add(row);
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

  static AggregateFunction getAggregateFunc(OneDBExpression exp) {
    if (exp instanceof OneDBAggCall) {
      switch (((OneDBAggCall) exp).getAggType()) {
        case COUNT:
          return new PlaintextCount((OneDBAggCall) exp);
        case SUM:
          return new PlaintextSum((OneDBAggCall) exp);
        case AVG:
          return new PlaintextAverage((OneDBAggCall) exp);
        case MAX:
          return new PlaintextMax((OneDBAggCall) exp);
        case MIN:
          return new PlaintextMin((OneDBAggCall) exp);
        default:
          throw new UnsupportedOperationException("Unsupport aggregation function");
      }
    } else {
      return PlaintextCombination.newBuilder(exp).build();
    }
  }

  static AggregateFunction getAggregateFunc(OneDBAggCall agg) {
    switch (agg.getAggType()) {
      case COUNT:
        return new PlaintextCount(agg);
      case SUM:
        return new PlaintextSum(agg);
      case AVG:
        return new PlaintextAverage(agg);
      case MAX:
        return new PlaintextMax(agg);
      case MIN:
        return new PlaintextMin(agg);
      default:
        throw new UnsupportedOperationException("Unsupport aggregation function");
    }
  }

  static class PlaintextSum implements AggregateFunction<Row, Comparable> {
    BigDecimal sum;
    int inputRef;

    PlaintextSum(OneDBAggCall agg) {
      this.sum = BigDecimal.valueOf(0);
      this.inputRef = ((OneDBAggCall) agg).getInputRef().get(0);
    }

    @Override
    public void add(Row ele) {
      sum = sum.add(number((Comparable) ele.getObject(inputRef)));
    }

    @Override
    public Comparable aggregate() {
      return sum;
    }
  }

  static class PlaintextCount implements AggregateFunction<Row, Comparable> {
    long count;

    PlaintextCount(OneDBAggCall agg) {
      this.count = 0;
    }

    @Override
    public void add(Row ele) {
      count++;
    }

    @Override
    public Comparable aggregate() {
      return count;
    }
  }

  static class PlaintextAverage implements AggregateFunction<Row, Comparable> {
    long count;
    BigDecimal sum;
    int inputRef;

    PlaintextAverage(OneDBAggCall agg) {
      this.count = 0;
      this.sum = BigDecimal.valueOf(0);
      this.inputRef = ((OneDBAggCall) agg).getInputRef().get(0);
    }

    @Override
    public void add(Row ele) {
      sum = sum.add(number((Comparable) ele.getObject(inputRef)));
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

  static class PlaintextMax implements AggregateFunction<Row, Comparable> {
    static Comparable MIN = new Comparable() {
      @Override
      public int compareTo(Object o) {
        return -1;
      }
    };

    Comparable maxValue;
    int inputRef;

    PlaintextMax(OneDBAggCall agg) {
      this.maxValue = MIN;
      this.inputRef = ((OneDBAggCall) agg).getInputRef().get(0);
    }

    @Override
    public void add(Row ele) {
      Comparable c = (Comparable) ele.getObject(inputRef);
      if (maxValue.compareTo(c) < 0) {
        maxValue = c;
      }
    }

    @Override
    public Comparable aggregate() {
      // todo: consider no value condition
      return maxValue;
    }
  }

  static class PlaintextMin implements AggregateFunction<Row, Comparable> {
    static Comparable MAX = new Comparable() {
      @Override
      public int compareTo(Object o) {
        return 1;
      }
    };

    Comparable minValue;
    int inputRef;

    PlaintextMin(OneDBAggCall agg) {
      this.minValue = MAX;
      this.inputRef = ((OneDBAggCall) agg).getInputRef().get(0);
    }

    @Override
    public void add(Row ele) {
      Comparable c = (Comparable) ele.getObject(inputRef);
      if (minValue.compareTo(c) > 0) {
        minValue = c;
      }
    }

    @Override
    public Comparable aggregate() {
      // todo: consider no value condition
      return minValue;
    }
  }

  static class PlaintextCombination implements AggregateFunction<Row, Comparable> {
    OneDBExpression exp;
    List<AggregateFunction<Row, Comparable>> in;

    private PlaintextCombination(OneDBExpression exp, List<AggregateFunction<Row, Comparable>> in) {
      this.exp = exp;
      this.in = in;
    }

    static PlaintextCombination.Builder newBuilder(OneDBExpression exp) {
      return new Builder(exp);
    }

    @Override
    public void add(Row ele) {
      for (AggregateFunction<Row, Comparable> agg : in) {
        agg.add(ele);
      }
    }

    @Override
    public Comparable aggregate() {
      RowBuilder inputRow = Row.newBuilder(in.size());
      for (int i = 0; i < in.size(); ++i) {
        inputRow.set(i, in.get(i).aggregate());
      }
      return ExpressionInterpreter.implement(inputRow.build(), exp);
    }

    static class Builder {
      OneDBExpression exp;
      List<AggregateFunction<Row, Comparable>> in;

      Builder(OneDBExpression exp) {
        this.exp = exp;
        in = new ArrayList<>();
      }

      PlaintextCombination build() {
        visit(exp);
        return new PlaintextCombination(exp, in);
      }

      void visit(OneDBExpression exp) {
        if (exp instanceof OneDBOperator) {
          List<OneDBExpression> children = ((OneDBOperator) exp).getInputs();
          for (int i = 0; i < children.size(); ++i) {
            OneDBExpression child = children.get(i);
            if (child instanceof OneDBAggCall) {
              int id = in.size();
              in.add(getAggregateFunc((OneDBAggCall) child));
              children.set(i, OneDBReference.fromIndex(child.getOutType(), id));
            } else {
              visit(child);
            }
          }
        }
      }
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
