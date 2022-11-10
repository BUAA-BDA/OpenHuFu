package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;

public class EmptyDataSet implements MaterializedDataSet {
  public final static EmptyDataSet INSTANCE = new EmptyDataSet();
  public final static DataSetProto PROTO = DataSetProto.newBuilder().build();

  @Override
  public Schema getSchema() {
    return Schema.EMPTY;
  }

  @Override
  public DataSetIterator getIterator() {
    return new DataSetIterator() {
      @Override
      public Object get(int columnIndex) {
        return null;
      }
      @Override
      public boolean next() {
        return false;
      }
      @Override
      public int size() {
        return 0;
      }
    };
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public int rowCount() {
    return 0;
  }

  @Override
  public Object get(int rowIndex, int columnIndex) {
    return null;
  }
}
