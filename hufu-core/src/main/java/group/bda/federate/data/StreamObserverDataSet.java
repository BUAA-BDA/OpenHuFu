package group.bda.federate.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import group.bda.federate.rpc.FederateCommon.DataSetProto;
import io.grpc.stub.StreamObserver;

public class StreamObserverDataSet extends StreamDataSet {
  private int BATCH_SIZE = 100;
  final private StreamObserver<DataSetProto> observer;
  private int count;
  private int rowCount;
  private List<Row> rows;
  private String uuid = "";

  public StreamObserverDataSet(final StreamObserver<DataSetProto> observer, final DataSet dataSet) {
    super(dataSet.header);
    uuid = dataSet.getUuid();
    this.observer = observer;
    count = 0;
    rowCount = 0;
    rows = new ArrayList<>();
    addRows(dataSet.rows);
  }

  public StreamObserverDataSet(final StreamObserver<DataSetProto> observer, final Header header) {
    super(header);
    this.observer = observer;
    count = 0;
    rowCount = 0;
    rows = new ArrayList<>();
  }

  public StreamObserverDataSet(final StreamObserver<DataSetProto> observer, final int batchSize, final Header header) {
    super(header);
    this.observer = observer;
    BATCH_SIZE = batchSize;
    count = 0;
    rowCount = 0;
    rows = new ArrayList<>();
  }

  private void send() {
    count = 0;
    observer.onNext(DataSet.newDataSetUnsafe(header, rows, uuid).toProto());
    rows.clear();
  }

  private void flush() {
    if (count != 0) {
      send();
    }
  }

  @Override
  public int getRowCount() {
    return rowCount;
  }

  @Override
  public void addRow(Row row) {
    rows.add(row);
    count++;
    rowCount++;
    if (count >= BATCH_SIZE) {
      send();
    }
  }

  @Override
  public void addRows(List<Row> rows) {
    int rowNumber = rows.size();
    int stepNumber = rowNumber / BATCH_SIZE;
    int restNumber = rowNumber % BATCH_SIZE;
    rowCount += rows.size();
    flush();
    for (int i = 0; i < stepNumber; ++i) {
      this.rows.addAll(rows.subList(i * BATCH_SIZE, (i + 1) * BATCH_SIZE));
      send();
    }
    if (restNumber != 0) {
      this.rows.addAll(rows.subList(rowNumber - restNumber, rowNumber));
      send();
    }
  }

  public void addDataSet(DataSet dataSet) {
    Iterator<Row> iterator = dataSet.rawIterator();
    int rowNumber = dataSet.rowCount();
    int stepNumber = rowNumber / BATCH_SIZE;
    int restNumber = rowNumber % BATCH_SIZE;
    rowCount += dataSet.rowCount();
    flush();
    for (int i = 0; i < stepNumber; ++i) {
      for (int j = 0; j < BATCH_SIZE; j++) {
        this.rows.add(iterator.next());
      }
      send();
    }
    if (restNumber != 0) {
      while (iterator.hasNext()) {
        this.rows.add(iterator.next());
      }
      send();
    }
  }

  @Override
  public void close() {
    flush();
    observer.onCompleted();
  }
}
