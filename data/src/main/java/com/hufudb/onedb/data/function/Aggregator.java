package com.hufudb.onedb.data.function;

import com.hufudb.onedb.data.storage.Row;

public abstract interface Aggregator extends AggregateFunction<Row, Row> {
  public abstract boolean hasNext();
}
