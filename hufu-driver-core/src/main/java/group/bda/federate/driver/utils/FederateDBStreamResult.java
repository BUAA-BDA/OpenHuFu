package group.bda.federate.driver.utils;

import java.util.List;

import group.bda.federate.rpc.FederateCommon.FederateDataSet;
import io.grpc.stub.StreamObserver;

public class FederateDBStreamResult {
  private FederateDataSet.Builder resultBuilder;
  private int BATCH_SIZE = 1000;
  final StreamObserver<FederateDataSet> observer;
  private int count;

  public FederateDBStreamResult(final StreamObserver<FederateDataSet> observer) {
    resultBuilder = FederateDataSet.newBuilder();
    this.observer = observer;
    count = 0;
  }

  public FederateDBStreamResult(final int batchSize, final StreamObserver<FederateDataSet> observer) {
    resultBuilder = FederateDataSet.newBuilder();
    BATCH_SIZE = batchSize;
    this.observer = observer;
    count = 0;
  }

  private void send() {
    count = 0;
    observer.onNext(resultBuilder.build());
    resultBuilder.clear();
  }

  public synchronized void addRow(String row) {
    resultBuilder.addResults(row);
    count++;
    if (count >= BATCH_SIZE) {
      send();
    }
  }

  public void addRows(List<String> rows) {
    int rowNumber = rows.size();
    int stepNumber = rowNumber / BATCH_SIZE;
    int restNumber = rowNumber % BATCH_SIZE;
    synchronized (this) {
      if (count != 0) {
        send();
      }
      for (int i = 0; i < stepNumber; ++i) {
        resultBuilder.addAllResults(rows.subList(i * BATCH_SIZE, (i + 1) * BATCH_SIZE));
        send();
      }
      if (restNumber != 0) {
        resultBuilder.addAllResults(rows.subList(rowNumber - restNumber, rowNumber));
        send();
      }
    }
  }

  public synchronized void complete() {
    if (count != 0) {
      send();
    }
    observer.onCompleted();
  }
}
