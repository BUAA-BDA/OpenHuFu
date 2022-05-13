package com.hufudb.onedb.data.storage;

import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.proto.OneDBData.ColumnProto;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;
import com.hufudb.onedb.data.schema.Schema;

/**
 * Dataset which store data in protobuf, represented as data source, Immutable.
 */
final public class ProtoDataSet implements DataSet {
  private final Schema schema;
  private final List<ProtoColumn> columns;
  private final int rowCount;

  ProtoDataSet(DataSetProto dataSet) {
    assert dataSet.getSchema().getColumnDescCount() == dataSet.getColumnCount();
    this.schema = Schema.fromProto(dataSet.getSchema());
    ImmutableList.Builder<ProtoColumn> cBuilder = ImmutableList.builder();
    for (int i = 0; i < dataSet.getColumnCount(); ++i) {
      cBuilder.add(new ProtoColumn(schema.getType(i), dataSet.getColumn(i)));
    }
    this.columns = cBuilder.build();
    if (columns.isEmpty()) {
      this.rowCount = 0;
    } else {
      this.rowCount = columns.get(0).size();
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

  public static DataSet materalize(DataSet dataSet) {
    Builder builder = new Builder(dataSet.getSchema());
    DataSetIterator it = dataSet.getIterator();
    while (it.next()) {
      builder.addRow(it);
    }
    return builder.build();
  }

  public static Builder newBuilder(Schema schema) {
    return new Builder(schema);
  }

  final class SourceIterator implements DataSetIterator {
    int pointer;

    SourceIterator() {
      pointer = -1;
    }

    @Override
    public boolean next() {
      pointer++;
      return pointer < rowCount;
    }

    @Override
    public Object get(int columnIndex) {
      return columns.get(columnIndex).getObject(pointer);
    }
  }

  public static final class Builder {
    final Schema schema;
    final List<ProtoColumn.Builder> columns;
    final int columnSize;

    Builder(Schema schema) {
      this.schema = schema;
      this.columnSize = schema.getColumnDescs().size();
      ImmutableList.Builder<ProtoColumn.Builder> cBuilders = ImmutableList.builder();
      schema.getColumnDescs().forEach(col -> cBuilders.add(ProtoColumn.newBuilder(col.getType())));
      this.columns = cBuilders.build();
    }

    public void addRow(Row row) {
      for (int i = 0; i < columnSize; ++i) {
        columns.get(i).add(row.get(i));
      }
    }

    public void clear() {
      columns.stream().forEach(c -> c.clear());
    }

    public DataSetProto buildProto() {
      List<ColumnProto> columnProtos = columns.stream().map(c -> c.buildProto()).collect(Collectors.toList());
      return DataSetProto.newBuilder().setSchema(schema.toProto()).addAllColumn(columnProtos).build();
    }

    public ProtoDataSet build() {
      return new ProtoDataSet(buildProto());
    }
  }
}
