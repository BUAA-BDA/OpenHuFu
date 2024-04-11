package com.hufudb.openhufu.data.function;

import com.hufudb.openhufu.data.storage.Row;

public interface Aggregator extends AggregateFunction<Row, Row> {
  boolean hasNext();
}
