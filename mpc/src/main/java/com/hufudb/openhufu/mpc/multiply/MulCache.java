package com.hufudb.openhufu.mpc.multiply;

import java.util.Random;

public class MulCache {
  private final int[][] ran;
  private long[][] val;
  private boolean isInit;
  private final static int TIME_OUT = 60000;

  public MulCache(int n) {
    ran = new int[n][2];
    val = new long[n][2];
    isInit = false;
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < n; ++i) {
      ran[i][0] = random.nextInt(128) + 1;
      ran[i][1] = random.nextInt(128) + 1;
    }
  }

  public int getRan(int idx, boolean isFirst) {
    return isFirst ? ran[idx][0] : ran[idx][1];
  }

  public synchronized void setVal(long[][] val) {
    this.val = val;
    isInit = true;
    this.notifyAll();
  }

  public int ranSum(int idx) {
    int sum = 0;
    for (int i = 0; i < ran.length; ++i) {
      if (i != idx) {
        sum += ran[i][0] * ran[i][1];
      }
    }
    return sum;
  }

  public synchronized long getVal(int idx, boolean isFirst) {
    int i = 0;
    while (!isInit && i < 20) {
      try {
        this.wait(TIME_OUT);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      ++i;
    }
    return isFirst ? val[idx][0] : val[idx][1];
  }
}
