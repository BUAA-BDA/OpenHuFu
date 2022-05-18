package com.hufudb.onedb.owner.storage;

import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.data.storage.ProtoDataSet;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;
import io.grpc.stub.StreamObserver;

/**
 * send data from source to StreamObserver, use {@link #stream() stream} method
 */
public class StreamDataSet implements DataSet {
  private final static int MAX_SIZE = 1000;

  private final DataSet source;
  private final StreamObserver<DataSetProto> observer;
  long count;

  public StreamDataSet(DataSet source, StreamObserver<DataSetProto> observer) {
    this.source = source;
    this.observer = observer;
    this.count = 0;
  }

  /**
   * send dataset in stream, call this method only once
   */
  public void stream() {
    DataSetIterator it = source.getIterator();
    ProtoDataSet.Builder builder = ProtoDataSet.newBuilder(getSchema());
    while (it.next()) {
      builder.addRow(it);
      count++;
      if (count % MAX_SIZE == 0) {
        send(builder.buildProto());
        builder.clear();
      }
    }
    if (count % MAX_SIZE != 0) {
      send(builder.buildProto());
    }
  }

  private void send(DataSetProto proto) {
    LOG.debug("Send %d rows in total");
    observer.onNext(proto);
  }

  @Override
  public Schema getSchema() {
    return source.getSchema();
  }

  @Override
  public DataSetIterator getIterator() {
    return EmptyDataSet.INSTANCE.getIterator();
  }

  @Override
  public void close() {
    LOG.info("Send {} rows with schema {}", count, source.getSchema());
    observer.onCompleted();
  }

  class StreamIterator implements DataSetIterator {
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
  }
}
