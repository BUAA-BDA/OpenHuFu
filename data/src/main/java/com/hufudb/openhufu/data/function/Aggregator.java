package com.hufudb.openhufu.data.function;

import com.hufudb.openhufu.data.storage.Row;

public abstract interface Aggregator extends AggregateFunction<Row, Row> {
  public abstract boolean hasNext();
}
