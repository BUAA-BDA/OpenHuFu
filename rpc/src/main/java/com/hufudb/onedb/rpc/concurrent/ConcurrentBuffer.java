package com.hufudb.onedb.rpc.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentBuffer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentBuffer.class);
  private static final long WAIT_TIMEOUT = 500000;

  private final static int DEFAULT_OFFSET = 8;
  private final Object buff[];
  private final Map<K, Integer> searchIndex;
  private final int mask;
  private final ReadWriteLock lock;
  private final Condition condition;
  private int wpoint;

  /**
   * create a buffer with 1 << offset slot
   */
  public ConcurrentBuffer(int offset) {
    this.buff = new Object[1 << offset];
    this.searchIndex = new HashMap<>();
    this.mask = (1 << offset) - 1;
    this.lock = new ReentrantReadWriteLock();
    this.condition = lock.writeLock().newCondition();
  }

  public ConcurrentBuffer() {
    this(DEFAULT_OFFSET);
  }

  public boolean put(K key, V value) {
    lock.writeLock().lock();
    if (buff[wpoint] != null) {
      LOG.warn("Buffer full");
      lock.writeLock().unlock();
      return false;
    } else {
      buff[wpoint] = value;
      searchIndex.put(key, wpoint);
      wpoint = (wpoint + 1) & mask;
      condition.signalAll();
      lock.writeLock().unlock();
      return true;
    }
  }

  // block the thread if header not found
  public V blockingPop(K key) {
    V target = null;
    int idx = -1;
    lock.writeLock().lock();
    try {
      while (!searchIndex.containsKey(key)) {
        if (!condition.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS)) {
          if (searchIndex.containsKey(key)) break;
          LOG.warn("Wait timeout for {}", key);
          return null;
        }
      }
      idx = searchIndex.get(key);
      target = (V) buff[idx];
    } catch (InterruptedException e) { // NOSONAR
      LOG.error("Error when waiting for packet: {}", e.getMessage());
    } finally {
      lock.writeLock().unlock();
    }
    if (target != null) {
      lock.writeLock().lock();
      searchIndex.remove(key);
      buff[idx] = null;
      lock.writeLock().unlock();
    } else {
      LOG.warn("Get NULL for {}", key);
    }
    return target;
  }

  // return null if header not found
  public V pop(K key) {
    V target = null;
    int idx = -1;
    lock.readLock().lock();
    if (searchIndex.containsKey(key)) {
      idx = searchIndex.get(key);
      target = (V) buff[idx];
    }
    lock.readLock().unlock();
    if (target != null) {
      lock.writeLock().lock();
      searchIndex.remove(key);
      buff[idx] = null;
      lock.writeLock().unlock();
    }
    return target;
  }
}
