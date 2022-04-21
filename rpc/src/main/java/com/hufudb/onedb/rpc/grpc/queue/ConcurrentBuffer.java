package com.hufudb.onedb.rpc.grpc.queue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentBuffer.class);
  private final static int DEFAULT_OFFSET = 8;
  private final DataPacket buff[];
  private final Map<DataPacketHeader, Integer> searchIndex;
  private final int mask;
  private final ReadWriteLock lock;
  private int wpoint;

  public ConcurrentBuffer(int offset) {
    this.buff = new DataPacket[1 << offset];
    this.searchIndex = new HashMap<>();
    this.mask = (1 << offset) - 1;
    this.lock = new ReentrantReadWriteLock();
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
      lock.writeLock().unlock();
      return false;
    }
  }

  public DataPacket get(DataPacketHeader header) {
    lock.readLock().lock();
    DataPacket target = null;
    int idx = -1;
    if (searchIndex.containsKey(header)) {
      idx = searchIndex.get(header);
      target = buff[idx];
    }
    lock.readLock().unlock();
    if (target == null) {
      return null;
    } else {
      lock.writeLock().lock();
      buff[idx] = null;
      lock.writeLock().unlock();
      return target;
    }
  }
}
