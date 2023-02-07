package com.hufudb.openhufu.common;

import com.hufudb.openhufu.common.metrics.aspect.HandlingTime;

/**
 * @author yang.song
 * @date 2/5/23 12:09 PM
 */
public class Student {
  @HandlingTime(name = "sjz")
  public void test() throws InterruptedException {
    System.out.println("I am sleeping!");
    Thread.sleep(1000);
  }
}
