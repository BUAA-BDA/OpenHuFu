package com.hufudb.openhufu.data.storage;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.proto.OpenHuFuData.DataSetProto;

/**
 * Used for multiple productor one consumer scenario,
 * use {@link #newProducer() newProducer} before {@link #getIterator() getIterator}
 * productors used {@link #add(DataSetProto) add} to add dataset
 */
public class MultiSourceDataSet implements DataSet {
  final static long TIME_OUT = 1000000;
  final Schema schema;
  final Queue<DataSetProto> queue;
  int productorNum;
  final long timeout;
  final Lock lock;
  final Condition cond;

  MultiSourceDataSet(Schema schema, int productorNum, long timeout) {
    this.schema = schema;
    this.queue = new LinkedBlockingDeque<>();
    this.productorNum = productorNum;
    this.timeout = timeout;
    this.lock = new ReentrantLock();
    this.cond = lock.newCondition();
  }

  public MultiSourceDataSet(Schema schema, int productorNum) {
    this(schema, productorNum, TIME_OUT);
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
    lock.lock();
    if (!queue.isEmpty()) {
      lock.unlock();
      return true;
    } else if (productorNum == 0) {
      // all productors has complete
      lock.unlock();
      return false;
    } else {
      // wait for productor
      try {
        if(!cond.await(timeout, TimeUnit.MILLISECONDS)) {
          LOG.error("Next Timeout in multi source dataset");
        }
      } catch (InterruptedException e) { //NOSONAR
        LOG.error("Next failed in multi source dataset", e);
        lock.unlock();
        return false;
      }
      if (queue.isEmpty()) {
        LOG.error("Consumer time out");
        lock.unlock();
        return false;
      }
      lock.unlock();
      return nextProto();
    }
  }

  private DataSetProto getProto() {
    lock.lock();
    DataSetProto dataset = queue.poll();
    lock.unlock();
    return dataset;
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
      lock.lock();
      productorNum -= 1;
      lock.unlock();
    }
  }
}
