package com.hufudb.onedb.data.storage;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;

/**
 * Used for multiple productor one consumer scenario,
 * use {@link #newProducer() newProducer} before {@link #getIterator() getIterator}
 * productors used {@link #add(DataSetProto) add} to add dataset
 */
public class MultiSourceDataSet implements DataSet {
  final static int TIME_OUT = 1000000;
  final Schema schema;
  final Queue<DataSetProto> queue;
  final AtomicInteger productorNum;
  final Lock lock;
  final Condition cond;

  public MultiSourceDataSet(Schema schema, int productorNum) {
    this.schema = schema;
    this.queue = new ConcurrentLinkedDeque<>();
    this.productorNum = new AtomicInteger(productorNum);
    this.lock = new ReentrantLock();
    this.cond = lock.newCondition();
  }

  public Producer newProducer() {
    return new Producer();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new Iterator();
  }

  @Override
  public void close() {
    // do nothing
  }

  private boolean nextProto() {
    if (!queue.isEmpty()) {
      return true;
    } else if (productorNum.get() == 0) {
      // all productors has complete
      return false;
    } else {
      // wait for productor
      try {
        lock.lock();
        cond.await(TIME_OUT, TimeUnit.MILLISECONDS);
        lock.unlock();
      } catch (Exception e) {
        LOG.error("Next failed in mutlisource dataset");
        e.printStackTrace();
        return false;
      }
      if (queue.isEmpty()) {
        LOG.error("Consumer time out");
        return false;
      }
      return nextProto();
    }
  }

  private DataSetProto getProto() {
    return queue.poll();
  }

  class Iterator implements DataSetIterator {
    DataSetIterator it = EmptyDataSet.INSTANCE.getIterator();

    @Override
    public Object get(int columnIndex) {
      return it.get(columnIndex);
    }

    @Override
    public boolean next() {
      if (it.next()) {
        return true;
      } else if (nextProto()) {
        ProtoDataSet dataSet = ProtoDataSet.create(getProto());
        if (!dataSet.getSchema().equals(schema)) {
          LOG.error("Unmatch schema in mutlisource dataset: expect [{}], get[{}]", dataSet.getSchema(), schema);
          throw new RuntimeException("Unmatch schema in mutlisource dataset");
        }
        it = dataSet.getIterator();
        return next();
      } else {
        return false;
      }
    }

    @Override
    public int size() {
      return schema.size();
    }
  }

  public class Producer {
    private Producer() {}

    public void add(DataSetProto proto) {
      lock.lock();
      queue.offer(proto);
      cond.signalAll();
      lock.unlock();
    }

    public void finish() {
      productorNum.decrementAndGet();
    }
  }
}
