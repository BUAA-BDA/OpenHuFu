package com.hufudb.onedb.expression;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.function.AggregateFunction;
import com.hufudb.onedb.data.storage.ArrayRow;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.interpreter.Interpreter;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;

public class AggregateFunctions {
  public static AggregateFunction<Row, Comparable> createAggregateFunction(Expression exp) {
    if (exp.getOpType().equals(OperatorType.AGG_FUNC)) {
      switch (AggFuncType.of(exp.getI32())) {
        case GROUPKEY:
          return new BasicGroupKey(exp);
        case COUNT:
          return new BasicCnt(exp);
        case SUM:
          return new BasicSum(exp);
        case AVG:
          return new BasicAvg(exp);
        case MAX:
          return new BasicMax(exp);
        case MIN:
          return new BasicMin(exp);
        default:
          throw new UnsupportedOperationException("Unsupport aggregation function");
      }
    } else {
      return BasicCombination.newBuilder(exp).build();
    }
  }

  public static List<AggregateFunction<Row, Comparable>> createAggregateFunction(
      List<Expression> aggs) {
    return aggs.stream().map(agg -> createAggregateFunction(agg)).collect(Collectors.toList());
  }

  public static class BasicGroupKey implements AggregateFunction<Row, Comparable> {
    Comparable key;
    final int inputRef;

    BasicGroupKey(Expression agg) {
      this.key = null;
      this.inputRef = agg.getIn(0).getI32();
    }

    BasicGroupKey(int inputRef) {
      this.key = null;
      this.inputRef = inputRef;
    }

    @Override
    public void add(Row ele) {
      key = (Comparable) ele.get(inputRef);
    }

    @Override
    public Comparable aggregate() {
      return key;
    }

    @Override
    public AggregateFunction<Row, Comparable> copy() {
      return new BasicGroupKey(inputRef);
    }
  }

  public static class BasicSum implements AggregateFunction<Row, Comparable> {
    BigDecimal sum;
    final int inputRef;
    final boolean distinct;
    final Set<Object> distinctSet;

    BasicSum(int inputRef, boolean distinct) {
      this.sum = BigDecimal.valueOf(0);
      this.inputRef = inputRef;
      this.distinct = distinct;
      if (distinct) {
        this.distinctSet = new HashSet<>();
      } else {
        this.distinctSet = null;
      }
    }

    BasicSum(Expression agg) {
      this(agg.getIn(0).getI32(), AggFuncType.isDistinct(agg.getI32()));
    }

    BasicSum(int inputRef) {
      this(inputRef, false);
    }

    @Override
    public void add(Row ele) {
      Object e = ele.get(inputRef);
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
    public AggregateFunction<Row, Comparable> copy() {
      return new BasicSum(inputRef, distinct);
    }
  }

  public static class BasicCnt implements AggregateFunction<Row, Comparable> {
    long count;
    final List<Integer> inputRefs;
    final int inputSize;
    final boolean distinct;
    final Set<Object> distinctSet;

    BasicCnt(List<Integer> inputRefs, boolean distinct) {
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

    BasicCnt(Expression agg) {
      this(agg.getInList().stream().map(e -> e.getI32()).collect(Collectors.toList()),
          AggFuncType.isDistinct(agg.getI32()));
    }

    @Override
    public void add(Row ele) {
      if (distinct) {
        ArrayRow.Builder builder = ArrayRow.newBuilder(inputSize);
        for (int i = 0; i < inputSize; ++i) {
          builder.set(i, ele.get(inputRefs.get(i)));
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
    public AggregateFunction<Row, Comparable> copy() {
      return new BasicCnt(inputRefs, distinct);
    }
  }

  public static class BasicAvg implements AggregateFunction<Row, Comparable> {
    long count;
    BigDecimal sum;
    final int inputRef;
    final boolean distinct;
    final Set<Object> distinctSet;

    BasicAvg(int inputRef, boolean distinct) {
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

    BasicAvg(Expression agg) {
      this(agg.getIn(0).getI32(), AggFuncType.isDistinct(agg.getI32()));
    }

    @Override
    public void add(Row ele) {
      Object e = ele.get(inputRef);
      if (distinct) {
        if (distinctSet.contains(e)) {
          return;
        }
        distinctSet.add(e);
      }
      sum = sum.add(number((Comparable) ele.get(inputRef)));
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
    public AggregateFunction<Row, Comparable> copy() {
      return new BasicAvg(inputRef, distinct);
    }
  }

  public static class BasicMax implements AggregateFunction<Row, Comparable> {
    static Comparable MIN = new Comparable() {
      @Override
      public int compareTo(Object o) {
        return -1;
      }
    };

    Comparable maxValue;
    final int inputRef;

    BasicMax(int inputRef) {
      this.maxValue = MIN;
      this.inputRef = inputRef;
    }

    BasicMax(Expression agg) {
      this(agg.getIn(0).getI32());
    }

    @Override
    public void add(Row ele) {
      Comparable c = (Comparable) ele.get(inputRef);
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
    public AggregateFunction<Row, Comparable> copy() {
      return new BasicMax(inputRef);
    }
  }

  public static class BasicMin implements AggregateFunction<Row, Comparable> {
    static Comparable MAX = new Comparable() {
      @Override
      public int compareTo(Object o) {
        return 1;
      }
    };

    Comparable minValue;
    int inputRef;

    BasicMin(int inputRef) {
      this.inputRef = inputRef;
      this.minValue = MAX;
    }

    BasicMin(Expression agg) {
      this(agg.getIn(0).getI32());
    }

    @Override
    public void add(Row ele) {
      Comparable c = (Comparable) ele.get(inputRef);
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
    public AggregateFunction<Row, Comparable> copy() {
      return new BasicMin(inputRef);
    }
  }

  public static class BasicCombination implements AggregateFunction<Row, Comparable> {
    final Expression exp;
    List<AggregateFunction<Row, Comparable>> in;

    private BasicCombination(Expression exp, List<AggregateFunction<Row, Comparable>> in) {
      this.exp = exp;
      this.in = in;
    }

    static BasicCombination.Builder newBuilder(Expression exp) {
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
      ArrayRow.Builder inputRow = ArrayRow.newBuilder(in.size());
      for (int i = 0; i < in.size(); ++i) {
        inputRow.set(i, in.get(i).aggregate());
      }
      return (Comparable) Interpreter.implement(inputRow.build(), exp);
    }

    public static class Builder {
      Expression exp;
      ImmutableList.Builder<AggregateFunction<Row, Comparable>> in;
      int inSize = 0;

      Builder(Expression exp) {
        this.exp = exp;
        this.in = ImmutableList.builder();
        this.inSize = 0;
      }

      BasicCombination build() {
        return new BasicCombination(visit(exp), in.build());
      }

      Expression visit(Expression exp) {
        if (exp.getInCount() > 0) {
          List<Expression> children = exp.getInList();
          Expression.Builder builder = exp.toBuilder();
          builder.clearIn();
          for (Expression child : children) {
            if (child.getOpType().equals(OperatorType.AGG_FUNC)) {
              int id = inSize;
              inSize++;
              in.add(createAggregateFunction(child));
              builder.addIn(
                  ExpressionFactory.createInputRef(id, child.getOutType(), child.getModifier()));
            } else {
              builder.addIn(visit(child));
            }
          }
          return builder.build();
        } else {
          return exp;
        }
      }
    }

    @Override
    public AggregateFunction<Row, Comparable> copy() {
      List<AggregateFunction<Row, Comparable>> inCopy =
          in.stream().map(i -> i.copy()).collect(Collectors.toList());
      return new BasicCombination(exp, inCopy);
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
