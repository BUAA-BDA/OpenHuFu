package com.hufudb.onedb.core.data;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hufudb.onedb.core.config.OneDBConfig;

public class StreamBuffer<E> implements Iterator<E> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamBuffer.class);

  private Queue<E> queue;
  private AtomicInteger productorNum;
  private Lock lock;

  public StreamBuffer(int productorNum) {
    queue = new ConcurrentLinkedQueue<E>();
    this.productorNum = new AtomicInteger(productorNum);
    this.lock = new ReentrantLock();
  }

  public void add(E e) {
    queue.offer(e);
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  /**
   * every productor can only call this function once
   */
  public void finish() {
    productorNum.decrementAndGet();
  }

  @Override
  public boolean hasNext() {
    if (!queue.isEmpty()) {
      return true;
    } else if (productorNum.get() == 0) {
      // all productors has complete
      return false;
    } else {
      // wait for productor
      try {
        synchronized (lock) {
          lock.wait(OneDBConfig.RPC_TIME_OUT);
        }
      } catch (Exception e) {
        return false;
      }
      if (queue.isEmpty()) {
        LOG.warn("iterator time out");
        return false;
      }
      return hasNext();
    }
  }

  @Override
  public E next() {
    return queue.poll();
  }
}
