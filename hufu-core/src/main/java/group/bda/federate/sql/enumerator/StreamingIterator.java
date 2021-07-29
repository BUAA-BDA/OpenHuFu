package group.bda.federate.sql.enumerator;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.config.FedSpatialConfig;

/**
 * a queue for multiple productors and one customer
 */
public class StreamingIterator<E> implements Iterator<E> {
  private static final Logger LOG = LogManager.getLogger(StreamingIterator.class);

  private Queue<E> queue;
  private AtomicInteger productorNum;
  private Lock lock;

  public StreamingIterator(int productorNum) {
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
          lock.wait(FedSpatialConfig.TIME_OUT);
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
