package com.hufudb.onedb.data.storage;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.OneDBData.DataSetProto;
import com.hufudb.onedb.data.schema.Schema;

/**
 * Dataset which store data in protobuf, represented as data source, Immutable.
 */
final public class ProtoDataSet implements DataSet {
  // encapsulation of protocolbuffer
  private final Schema schema;
  private final List<ProtoColumn> columns;
  private final int rowCount;

  ProtoDataSet(DataSetProto dataSet) {
    assert dataSet.getSchema().getColumnDescCount() == dataSet.getColumnsCount();
    this.schema = Schema.fromProto(dataSet.getSchema());
    ImmutableList.Builder<ProtoColumn> cBuilder = ImmutableList.builder();
    for (int i = 0; i < dataSet.getColumnsCount(); ++i) {
      cBuilder.add(new ProtoColumn(schema.getType(i), dataSet.getColumns(i)));
    }
    this.columns = cBuilder.build();
    if (columns.isEmpty()) {
      this.rowCount = 0;
    } else {
      this.rowCount = columns.size();
    }
  }

  public static ProtoDataSet create(DataSetProto dataSet) {
    return new ProtoDataSet(dataSet);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new SourceIterator();
  }

  @Override
  public void close() {
    // do nothing
  }

  public int size() {
    return rowCount;
  }

  final class SourceIterator implements DataSetIterator {
    int pointer;

    SourceIterator() {
      pointer = -1;
    }

    @Override
    public boolean hasNext() {
      pointer++;
      return pointer < rowCount;
    }

    @Override
    public Object get(int columnIndex) {
      return columns.get(columnIndex).getObject(pointer);
    }
  }
}
