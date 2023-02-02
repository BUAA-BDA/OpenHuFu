package com.hufudb.openhufu.expression;

import java.util.List;
import com.hufudb.openhufu.data.function.AggregateFunction;
import com.hufudb.openhufu.data.function.Aggregator;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.ArrayRow;
import com.hufudb.openhufu.data.storage.Row;
import com.hufudb.openhufu.interpreter.Interpreter;

/**
 * aggregator which aggregates multiple rows into a single row
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
