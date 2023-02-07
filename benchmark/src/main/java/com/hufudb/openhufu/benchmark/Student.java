package com.hufudb.openhufu.benchmark;

import com.hufudb.openhufu.benchmark.annotation.HandlingTime;

/**
 * @author yang.song
 * @date 2/5/23 12:09 PM
 */
public class Student {
  @HandlingTime
  public void test() throws InterruptedException {
    System.out.println("I am sleeping!");
    Thread.sleep(1000);
  }
}
