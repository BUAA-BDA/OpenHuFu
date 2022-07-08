package com.hufudb.onedb.data.storage.utils;

import com.hufudb.onedb.data.storage.Point;

public class PointUtils {
  public PointUtils() {
  }

  /**
   * convert com.hufudb.onedb.data.storage.Point to String
   */
  public String pointToStr(Point p) {
    return "Point(" + String.valueOf(p.getX()) + " " + String.valueOf(p.getY()) + ")";
  }

  /**
   * convert String to com.hufudb.onedb.data.storage.Point
   */
  public Point strToPoint(String s) {
    try {
      String tmp = s.substring(6, s.length()-1);
      String[] newStr = tmp.split("\\s+");
      return new Point(Double.valueOf(newStr[0]), Double.valueOf(newStr[1]));
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
