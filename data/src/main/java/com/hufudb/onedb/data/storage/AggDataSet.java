package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.function.Aggregator;
import com.hufudb.onedb.data.schema.Schema;

public class AggDataSet implements DataSet {
  final Schema schema;
  final Aggregator aggregator;
  final DataSet source;

  AggDataSet(Schema schema, Aggregator aggregator, DataSet source) {
    this.schema = schema;
    this.aggregator = aggregator;
    this.source = source;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return null;
  }

  @Override
  public void close() {
    source.close();
  }

  class AggIterator implements DataSetIterator {
    Row row;

    AggIterator() {
      aggregator.add(getIterator());
    }

    @Override
    public boolean hasNext() {
      boolean next = aggregator.hasNext();
      if (next) {
        row = aggregator.aggregate();
      }
      return next;
    }

    @Override
    public Object get(int columnIndex) {
      return row;
    }
  }
}
