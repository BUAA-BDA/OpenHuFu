package com.hufudb.openhufu.common;

import com.hufudb.openhufu.common.metrics.time.TimeManager;

import java.util.Map;

/**
 * @author yang.song
 * @date ${DATE} ${TIME}
 */
public class Main {

  public static void main(String[] args) throws InterruptedException {
    Student student = new Student();
    student.test();
    for (Map.Entry<String, Long> entry: TimeManager.getAllInfo().entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
  }
}