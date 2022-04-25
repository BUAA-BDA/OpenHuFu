package com.hufudb.onedb.rpc.grpc.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentBuffer.class);
  private static final long WAIT_TIMEOUT = 2000;

  private final static int DEFAULT_OFFSET = 8;
  private final DataPacket buff[];
  private final Map<DataPacketHeader, Integer> searchIndex;
  private final int mask;
  private final ReadWriteLock lock;
  private final Condition condition;
  private int wpoint;

  public ConcurrentBuffer(int offset) {
    this.buff = new DataPacket[1 << offset];
    this.searchIndex = new HashMap<>();
    this.mask = (1 << offset) - 1;
    this.lock = new ReentrantReadWriteLock();
    this.condition = lock.writeLock().newCondition();
  }

  public ConcurrentBuffer() {
    this(DEFAULT_OFFSET);
  }

  public boolean put(DataPacket packet) {
    lock.writeLock().lock();
    if (buff[wpoint] != null) {
      LOG.warn("Buffer full");
      lock.writeLock().unlock();
      return true;
    } else {
      buff[wpoint] = packet;
      searchIndex.put(packet.getHeader(), wpoint);
      wpoint = (wpoint + 1) & mask;
      condition.signalAll();
      lock.writeLock().unlock();
      return false;
    }
  }

  // block the thread if header not found
  public DataPacket blockingPop(DataPacketHeader header) {
    DataPacket target = null;
    int idx = -1;
    lock.writeLock().lock();
    try {
      while (!searchIndex.containsKey(header)) {
        if (!condition.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS)) {
          if (searchIndex.containsKey(header)) break;
          LOG.warn("Wait timeout for packet {}", header);
          return null;
        }
      }
      idx = searchIndex.get(header);
      target = buff[idx];
    } catch (InterruptedException e) {
      LOG.error("Error when waiting for packet: {}", e.getMessage());
    } finally {
      lock.writeLock().unlock();
    }
    if (target != null) {
      lock.writeLock().lock();
      buff[idx] = null;
      lock.writeLock().unlock();
    }
    return target;
  }

  // return null if header not found
  public DataPacket pop(DataPacketHeader header) {
    DataPacket target = null;
    int idx = -1;
    lock.readLock().lock();
    if (searchIndex.containsKey(header)) {
      idx = searchIndex.get(header);
      target = buff[idx];
    }
    lock.readLock().unlock();
    if (target != null) {
      lock.writeLock().lock();
      buff[idx] = null;
      lock.writeLock().unlock();
    }
    return target;
  }
}
