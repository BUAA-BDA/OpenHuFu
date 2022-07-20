package com.hufudb.onedb.expression;

import java.util.List;
import com.hufudb.onedb.data.function.AggregateFunction;
import com.hufudb.onedb.data.function.Aggregator;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.ArrayRow;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.interpreter.Interpreter;

/**
 * SingleAggregator会在Plan的groups.size()==0时启用
 * 同时GroupAggregator也依赖于它
 * 其管理着一组AggregateFunction
 * 每个AggregateFunction计算获得一个值，最后组成一行（即Row）并返回
 */
public class SingleAggregator implements Aggregator {
  final Schema schema;
  final List<AggregateFunction<Row, Comparable>> aggFunc;
  int size;

  public SingleAggregator(Schema schema, List<AggregateFunction<Row, Comparable>> aggFunc) {
    this.schema = schema;
    this.aggFunc = aggFunc;
    this.size = 0;
  }

  @Override
  public void add(Row row) {
    for (AggregateFunction<Row, Comparable> func : aggFunc) {
      func.add(row);
    }
  }

  /**
   * 根据给定的AggregateFunction集合，获得结果集合Row
   * 
   * get result as a Row based on the given AggregateFunction(s)
   */
  @Override
  public Row aggregate() {
    ArrayRow.Builder builder = ArrayRow.newBuilder(aggFunc.size());
    for (int i = 0; i < aggFunc.size(); ++i) {
      builder.set(i, Interpreter.cast(schema.getType(i), aggFunc.get(i).aggregate()));
    }
    return builder.build();
  }

  @Override
  public boolean hasNext() {
    size++;
    return size < 2;
  }

  @Override
  public SingleAggregator copy() {
    return new SingleAggregator(schema, AggregateFunction.copy(aggFunc));
  }
}
