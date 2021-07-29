package group.bda.federate.driver.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


import group.bda.federate.config.FedSpatialConfig;
import group.bda.federate.data.DataSet;
import group.bda.federate.security.secretsharing.utils.MulCache;
import group.bda.federate.security.secretsharing.utils.ShamirCache;

public class ConcurrentBuffer {
  private final Map<String, Object> bufferMap;
  private final Lock[] locks;
  private final int RETRY;

  public ConcurrentBuffer() {
    bufferMap = new ConcurrentHashMap<>();
    locks = new Lock[FedSpatialConfig.LOCK_NUMBER];
    for (int i = 0; i < FedSpatialConfig.LOCK_NUMBER; ++i) {
      locks[i] = new ReentrantLock();
    }
    RETRY = FedSpatialConfig.RETRY;
  }

  public void set(String uuid, Object buffer) {
    int hash = quickHash(uuid.charAt(0), uuid.charAt(uuid.length() - 1));
    synchronized (locks[hash]) {
      bufferMap.put(uuid, buffer);
      locks[hash].notifyAll();
    }
  }

  public boolean contains(String uuid) {
    return bufferMap.containsKey(uuid);
  }

  public Object get(String uuid) {
    int count = 0;
    int hash = quickHash(uuid.charAt(0), uuid.charAt(uuid.length() - 1));
    synchronized (locks[hash]) {
      while (!bufferMap.containsKey(uuid) && count < RETRY) {
        try {
          locks[hash].wait(FedSpatialConfig.TIME_OUT);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return null;
        }
      count++;
      }
    }
    return bufferMap.get(uuid);
  }

  public MulCache getMulCache(String uuid) {
    return (MulCache) get(uuid);
  }

  public AggCache getAggCache(String uuid) {
    return (AggCache) get(uuid);
  }

  public ShamirCache getShamirCache(String uuid) {
    return (ShamirCache) get(uuid);
  }

  public DataSet getDataSet(String uuid) {
    return (DataSet) get(uuid);
  }

  public DistanceDataSet getDistanceDataSet(String uuid) {
    return (DistanceDataSet) get(uuid);
  }

  public void remove(String uuid) {
    bufferMap.remove(uuid);
  }

  private int quickHash(char b, char e) {
    return ((int) b * (int) e + (int) e) % locks.length;
  }
}
