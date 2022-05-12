package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.function.Filter;
import com.hufudb.onedb.data.schema.Schema;

public class FilterDataSet implements DataSet {
  private final Schema schema;
  private final Filter filter;
  private final DataSet source;

  public FilterDataSet(Schema schema, Filter filter, DataSet source) {
    this.schema = schema;
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
    public boolean hasNext() {
      while (iterator.hasNext()) {
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
  }
}
