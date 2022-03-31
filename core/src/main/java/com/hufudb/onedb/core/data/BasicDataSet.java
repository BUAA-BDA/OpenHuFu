package com.hufudb.onedb.core.data;

import com.google.protobuf.ByteString;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.RowsProto;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SerializationUtils;

public class BasicDataSet implements EnumerableDataSet {
  protected Header header;
  protected List<Row> rows;
  protected int cursor = 0;
  protected Row current = null;

  protected BasicDataSet(Header header, List<Row> rows) {
    this.header = header;
    this.rows = rows;
  }

  protected BasicDataSet(Header header) {
    this.header = header;
    rows = new ArrayList<>();
  }

  public DataSetProto toProto() {
    DataSetProto.Builder proto = DataSetProto.newBuilder();
    proto.setHeader(header.toProto());
    RowsProto.Builder rowsProto = RowsProto.newBuilder();
    rows.stream()
        .forEach(row -> rowsProto.addRow(ByteString.copyFrom(SerializationUtils.serialize(row))));
    return proto.setRows(rowsProto).build();
  }

  public static BasicDataSet fromProto(DataSetProto proto) {
    Header header = Header.fromProto(proto.getHeader());
    RowsProto rowsProto = proto.getRows();
    List<Row> rows =
        rowsProto.getRowList().stream()
            .map(bytes -> (Row) SerializationUtils.deserialize(bytes.toByteArray()))
            .collect(Collectors.toList());
    return new BasicDataSet(header, rows);
  }

  public static BasicDataSet of(Header header) {
    return new BasicDataSet(header);
  }

  @Override
  public Header getHeader() {
    return header;
  }

  @Override
  public Row current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    if (cursor >= rows.size()) {
      return false;
    } else {
      current = rows.get(cursor);
      cursor++;
      return true;
    }
  }

  @Override
  public void reset() {
    cursor = 0;
    current = null;
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public int getRowCount() {
    return rows.size();
  }

  @Override
  public void addRow(Row row) {
    rows.add(row);
  }

  @Override
  public void addRows(List<Row> rows) {
    rows.addAll(rows);
  }

  @Override
  public void mergeDataSet(DataSet dataSet) {
    rows.addAll(dataSet.getRows());
  }

  @Override
  public List<Row> getRows() {
    return rows;
  }
}
