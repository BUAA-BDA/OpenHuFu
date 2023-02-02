package com.hufudb.openhufu.data.storage;

import com.hufudb.openhufu.data.function.Aggregator;
import com.hufudb.openhufu.data.schema.Schema;

public class AggDataSet implements DataSet {
  final Schema schema;
  final Aggregator aggregator;
  final DataSet source;

  AggDataSet(Schema schema, Aggregator aggregator, DataSet source) {
    this.schema = schema;
    this.aggregator = aggregator;
    this.source = source;
  }

  public static AggDataSet create(Schema schema, Aggregator aggregator, DataSet source) {
    return new AggDataSet(schema, aggregator, source);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new Iterator();
  }

  @Override
  public void close() {
    source.close();
  }

  class Iterator implements DataSetIterator {
    Row row;

    Iterator() {
      materialize();
    }

    void materialize() {
      DataSetIterator it = source.getIterator();
      while (it.next()) {
        aggregator.add(it);
      }
    }

    @Override
    public boolean next() {
      boolean next = aggregator.hasNext();
      if (next) {
        row = aggregator.aggregate();
      }
      return next;
    }

    @Override
    public Object get(int columnIndex) {
      return row.get(columnIndex);
    }

    @Override
    public int size() {
      return schema.size();
    }
  }
}
