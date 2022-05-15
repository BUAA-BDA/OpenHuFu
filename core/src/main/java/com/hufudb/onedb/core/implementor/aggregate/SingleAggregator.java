package com.hufudb.onedb.core.implementor.aggregate;

import java.util.List;
import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.Row.RowBuilder;
import com.hufudb.onedb.core.implementor.plaintext.PlaintextInterpreter;

public class SingleAggregator implements Aggregator {

  final List<AggregateFunction<Row, Comparable>> aggFunc;
  final List<ColumnType> types;
  boolean next;

  SingleAggregator(List<AggregateFunction<Row, Comparable>> aggFunc, List<ColumnType> types) {
    this.aggFunc = aggFunc;
    this.types = types;
    this.next = true;
  }

  @Override
  public void add(Row row) {
    for (AggregateFunction<Row, Comparable> func : aggFunc) {
      func.add(row);
    }
  }

  @Override
  public Row aggregate() {
    RowBuilder builder = Row.newBuilder(aggFunc.size());
    for (int i = 0; i < aggFunc.size(); ++i) {
      builder.set(i, PlaintextInterpreter.cast(aggFunc.get(i).aggregate(), types.get(i)));
    }
    return builder.build();
  }

  @Override
  public SingleAggregator patternCopy() {
    return new SingleAggregator(AggregateFunction.patternCopy(aggFunc), types);
  }

  @Override
  public boolean hasNext() {
    boolean next = this.next;
    this.next = false;
    return next;
  }

  @Override
  public void reset() {
    next = true;
  }

  @Override
  public List<ColumnType> getOutputTypes() {
    return types;
  }

  @Override
  public int size() {
    return aggFunc.size();
  }
}
