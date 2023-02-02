package com.hufudb.openhufu.data.storage;

import com.hufudb.openhufu.data.schema.Schema;

public class LimitDataSet implements DataSet {
  final Schema schema;
  final DataSet source;
  final int offset;
  final int fetch;

  LimitDataSet(DataSet source, int offset, int fetch) {
    this.schema = source.getSchema();
    this.source = source;
    this.offset = offset;
    this.fetch = fetch;
  }

  public static DataSet limit(DataSet source, int offset, int fetch) {
    return new LimitDataSet(source, offset, fetch);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new Iterator(source.getIterator());
  }

  @Override
  public void close() {
    source.close();
  }

  class Iterator implements DataSetIterator {
    final DataSetIterator iter;
    int pointer;

    Iterator(DataSetIterator iter) {
      this.iter = iter;
      pointer = 0;
      while (pointer < offset && iter.next()) {
        pointer++;
      }
      pointer = 0;
    }

    @Override
    public Object get(int columnIndex) {
      return iter.get(columnIndex);
    }

    @Override
    public int size() {
      return schema.size();
    }

    @Override
    public boolean next() {
      if (fetch != 0 && pointer >= fetch) {
        return false;
      }
      ++pointer;
      return iter.next();
    }
  }
}
