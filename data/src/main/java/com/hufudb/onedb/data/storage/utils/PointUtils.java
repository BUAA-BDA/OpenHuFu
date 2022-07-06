package com.hufudb.onedb.data.storage.utils;

import com.hufudb.onedb.data.storage.Point;

public class PointUtils {
  public PointUtils() {}

  /**
   * convert com.hufudb.onedb.data.storage.Point to String
   */
  public String pointToStr(Point p) {
    return String.valueOf(p.getX()) + " " +String.valueOf(p.getY());
  }

  /**
   * convert String to com.hufudb.onedb.data.storage.Point
   */
  public Point strToPoint(String s) {
    try {
      String[] newStr = s.split("\\s+");
      return new Point(Double.valueOf(newStr[0]), Double.valueOf(newStr[1]));
    } catch(Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}

