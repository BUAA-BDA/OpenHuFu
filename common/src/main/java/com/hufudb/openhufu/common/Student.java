package com.hufudb.openhufu.common.aspect;

import com.hufudb.openhufu.common.aspect.HandlingTime;

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
