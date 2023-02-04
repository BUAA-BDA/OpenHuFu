package com.hufudb.openhufu.data.storage;

import com.hufudb.openhufu.data.function.Filter;
import com.hufudb.openhufu.data.schema.Schema;

public class FilterDataSet implements DataSet {
  private final Schema schema;
  private final Filter filter;
  private final DataSet source;

  public FilterDataSet(DataSet source, Filter filter) {
    this.schema = source.getSchema();
    this.filter = filter;
    this.source = source;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new FilterIterator(source.getIterator());
  }

  @Override
  public void close() {
    source.close();
  }

  class FilterIterator implements DataSetIterator {
    DataSetIterator iterator;

    FilterIterator(DataSetIterator iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean next() {
      while (iterator.next()) {
        if (filter.filter(iterator)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public Object get(int columnIndex) {
      return iterator.get(columnIndex);
    }

    @Override
    public int size() {
      return schema.size();
    }
  }
}
