package com.hufudb.onedb.core.implementor.plaintext;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.Row.RowBuilder;
import com.hufudb.onedb.core.implementor.aggregate.AggregateFunction;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import com.hufudb.onedb.core.sql.expression.OneDBReference;

public class PlaintextAggregateFunctions {
  public static AggregateFunction getAggregateFunc(OneDBExpression exp) {
    if (exp instanceof OneDBAggCall) {
      switch (((OneDBAggCall) exp).getAggType()) {
        case GROUPKEY:
          return new PlaintextGroupKey((OneDBAggCall) exp);
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

  public static class PlaintextGroupKey implements AggregateFunction<Row, Comparable> {
    Comparable key;
    final int inputRef;

    PlaintextGroupKey(OneDBAggCall agg) {
      this.key = null;
      this.inputRef = ((OneDBAggCall) agg).getInputRef().get(0);
    }

    PlaintextGroupKey(int inputRef) {
      this.key = null;
      this.inputRef = inputRef;
    }

    @Override
    public void add(Row ele) {
      key = (Comparable) ele.getObject(inputRef);
    }

    @Override
    public Comparable aggregate() {
      return key;
    }

    @Override
    public AggregateFunction<Row, Comparable> patternCopy() {
      return new PlaintextGroupKey(inputRef);
    }
  }

  public static class PlaintextSum implements AggregateFunction<Row, Comparable> {
    BigDecimal sum;
    final int inputRef;
    final boolean distinct;
    final Set<Object> distinctSet;

    PlaintextSum(int inputRef, boolean distinct) {
      this.sum = BigDecimal.valueOf(0);
      this.inputRef = inputRef;
      this.distinct = distinct;
      if (distinct) {
        this.distinctSet = new HashSet<>();
      } else {
        this.distinctSet = null;
      }
    }

    PlaintextSum(OneDBAggCall agg) {
      this(agg.getInputRef().get(0), agg.isDistinct());
    }

    PlaintextSum(int inputRef) {
      this(inputRef, false);
    }

    @Override
    public void add(Row ele) {
      Object e = ele.getObject(inputRef);
      if (distinct) {
        if (distinctSet.contains(e)) {
          return;
        }
        distinctSet.add(e);
      }
      sum = sum.add(number((Comparable) e));
    }

    @Override
    public Comparable aggregate() {
      return sum;
    }

    @Override
    public AggregateFunction<Row, Comparable> patternCopy() {
      return new PlaintextSum(inputRef, distinct);
    }
  }

  public static class PlaintextCount implements AggregateFunction<Row, Comparable> {
    long count;
    final List<Integer> inputRefs;
    final int inputSize;
    final boolean distinct;
    final Set<Object> distinctSet;

    PlaintextCount(List<Integer> inputRefs, boolean distinct) {
      this.count = 0;
      this.inputRefs = inputRefs;
      this.inputSize = inputRefs.size();
      this.distinct = distinct;
      if (distinct) {
        this.distinctSet = new HashSet<>();
      } else {
        this.distinctSet = null;
      }
    }

    PlaintextCount(OneDBAggCall agg) {
      this(agg.getInputRef(), agg.isDistinct());
    }

    @Override
    public void add(Row ele) {
      if (distinct) {
        RowBuilder builder = Row.newBuilder(inputSize);
        for (int i = 0; i < inputSize; ++i) {
          builder.set(i, ele.getObject(inputRefs.get(i)));
        }
        Row r = builder.build();
        if (distinctSet.contains(r)) {
          return;
        }
        distinctSet.add(r);
      }
      count++;
    }

    @Override
    public Comparable aggregate() {
      return count;
    }

    @Override
    public AggregateFunction<Row, Comparable> patternCopy() {
      return new PlaintextCount(inputRefs, distinct);
    }
  }

  public static class PlaintextAverage implements AggregateFunction<Row, Comparable> {
    long count;
    BigDecimal sum;
    final int inputRef;
    final boolean distinct;
    final Set<Object> distinctSet;

    PlaintextAverage(int inputRef, boolean distinct) {
      this.count = 0;
      this.sum = BigDecimal.valueOf(0);
      this.inputRef = inputRef;
      this.distinct = distinct;
      if (distinct) {
        this.distinctSet = new HashSet<>();
      } else {
        this.distinctSet = null;
      }
    }

    PlaintextAverage(OneDBAggCall agg) {
      this(agg.getInputRef().get(0), agg.isDistinct());
    }

    @Override
    public void add(Row ele) {
      Object e = ele.getObject(inputRef);
      if (distinct) {
        if (distinctSet.contains(e)) {
          return;
        }
        distinctSet.add(e);
      }
      sum = sum.add(number((Comparable) ele.getObject(inputRef)));
      count++;
    }

    @Override
    public Comparable aggregate() {
      if (count == 0) {
        return 0;
      }
      return sum.divide(BigDecimal.valueOf(count), MathContext.DECIMAL64);
    }

    @Override
    public AggregateFunction<Row, Comparable> patternCopy() {
      return new PlaintextAverage(inputRef, distinct);
    }
  }

  public static class PlaintextMax implements AggregateFunction<Row, Comparable> {
    static Comparable MIN = new Comparable() {
      @Override
      public int compareTo(Object o) {
        return -1;
      }
    };

    Comparable maxValue;
    final int inputRef;

    PlaintextMax(int inputRef) {
      this.maxValue = MIN;
      this.inputRef = inputRef;
    }

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

    @Override
    public AggregateFunction<Row, Comparable> patternCopy() {
      return new PlaintextMax(inputRef);
    }
  }

  public static class PlaintextMin implements AggregateFunction<Row, Comparable> {
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

    PlaintextMin(int inputRef) {
      this.inputRef = inputRef;
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

    @Override
    public AggregateFunction<Row, Comparable> patternCopy() {
      return new PlaintextMin(inputRef);
    }
  }

  public static class PlaintextCombination implements AggregateFunction<Row, Comparable> {
    final OneDBExpression exp;
    List<AggregateFunction<Row, Comparable>> in;

    private PlaintextCombination(OneDBExpression exp, List<AggregateFunction<Row, Comparable>> in) {
      this.exp = exp;
      this.in = in;
    }

    static PlaintextCombination.Builder newBuilder(OneDBExpression exp) {
      return new Builder(exp);
    }

    public static PlaintextCombination.Builder newHorizontalParitionBuilder(OneDBExpression exp,
        List<OneDBAggCall> localAggCalls) {
      return new Builder(exp, localAggCalls);
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
      return PlaintextInterpreter.implement(inputRow.build(), exp);
    }

    @Override
    public AggregateFunction<Row, Comparable> patternCopy() {
      List<AggregateFunction<Row, Comparable>> inCopy =
          in.stream().map(i -> i.patternCopy()).collect(Collectors.toList());
      return new PlaintextCombination(exp, inCopy);
    }

    public static class Builder {
      OneDBExpression exp;
      List<AggregateFunction<Row, Comparable>> in;

      Builder(OneDBExpression exp) {
        this.exp = exp;
        in = new ArrayList<>();
      }

      Builder(OneDBExpression exp, List<OneDBAggCall> localAggCalls) {
        this.exp = exp;
        this.in = new ArrayList<>();
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
    return comparable instanceof BigDecimal ? (BigDecimal) comparable
        : comparable instanceof BigInteger ? new BigDecimal((BigInteger) comparable)
            : comparable instanceof Long || comparable instanceof Integer
                || comparable instanceof Short ? new BigDecimal(((Number) comparable).longValue())
                    : new BigDecimal(((Number) comparable).doubleValue());
  }
}
