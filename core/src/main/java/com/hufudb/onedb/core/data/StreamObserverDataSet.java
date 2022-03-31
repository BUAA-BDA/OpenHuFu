package com.hufudb.onedb.core.data;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;

public class StreamObserverDataSet implements DataSet {
  private int BATCH_SIZE = 100;
  private final StreamObserver<DataSetProto> observer;
  private int count;
  private int rowCount;
  private Header header;
  private List<Row> rows;

  public StreamObserverDataSet(final StreamObserver<DataSetProto> observer, Header header) {
    this.header = header;
    this.observer = observer;
    count = 0;
    rowCount = 0;
    rows = new ArrayList<>();
  }

  @Override
  public Header getHeader() {
    return header;
  }

  public void addRow(Row row) {
    rows.add(row);
    count++;
    rowCount++;
    if (count >= BATCH_SIZE) {
      send();
    }
  }

  public void addRows(List<Row> rows) {
    int size = rows.size();
    int i = 0;
    for (; i + BATCH_SIZE < size; i += BATCH_SIZE) {
      this.rows.addAll(rows.subList(i, i + BATCH_SIZE));
      send();
    }
    if (i < size) {
      this.rows.addAll(rows.subList(i, size));
      send();
    }
  }

  @Override
  public int getRowCount() {
    return rowCount;
  }

  @Override
  public void mergeDataSet(DataSet dataSet) {
    addRows(dataSet.getRows());
  }

  // can't get rows from stream dataset
  @Override
  public List<Row> getRows() {
    return ImmutableList.of();
  }

  private void send() {
    count = 0;
    BasicDataSet dataSet = new BasicDataSet(header, rows);
    DataSetProto proto = dataSet.toProto();
    observer.onNext(proto);
    dataSet.close();
    rows.clear();
  }

  private void flush() {
    if (count != 0) {
      send();
    }
  }

  public void close() {
    flush();
    observer.onCompleted();
  }
}
