package com.hufudb.onedb.data.storage;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Point {
  private static PointParser parser = new PointParser();

  private double x;
  private double y;

  public Point(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public double getX() {
    return this.x;
  }

  public double getY() {
    return this.y;
  }

  @Override
  public String toString() {
    return String.format("POINT(%f %f)", getX(), getY());
  }

  public byte[] toBytes() {
    byte[] tmp = ByteBuffer.allocate(Double.BYTES).putDouble(x).array();
    byte[] encoded = new byte[Double.BYTES * 2];
    for (int i = 0; i < Double.BYTES; ++i) {
      encoded[i] = tmp[i];
    }
    tmp = ByteBuffer.allocate(Double.BYTES).putDouble(y).array();
    for (int i = 0; i < Double.BYTES; ++i) {
        encoded[i + Double.BYTES] = tmp[i];
    }
    return encoded;
  }

  public static Point fromBytes(byte[] encoded) {
    byte[] tmp = new byte[Double.BYTES];
    for (int i = 0; i < Double.BYTES; ++i) {
      tmp[i] = encoded[i];
    }
    double x = ByteBuffer.wrap(tmp).getDouble();
    for (int i = 0; i < Double.BYTES; ++i) {
      tmp[i] = encoded[i + Double.BYTES];
    }
    double y = ByteBuffer.wrap(tmp).getDouble();
    return new Point(x, y);
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || (obj instanceof Point && ((Point) obj).getX() == x && ((Point) obj).getY() == y);
  }

  private static class PointParser {
    private final static Logger LOG = LoggerFactory.getLogger(PointParser.class);

    private static String pointRex = "\\(\\s*([\\-]?[0-9]+[.]?[0-9]*)\\s+([\\-]?[0-9]+[.]?[0-9]*)\\s*\\)";
    private Pattern pointPattern;

    PointParser() {
      pointPattern = Pattern.compile(pointRex);
    }

    Point parse(String pointStr) {
      if (pointStr == null) {
        return null;
      }
      Matcher pointMatcher = pointPattern.matcher(pointStr);
      if (pointMatcher.find()) {
        String xStr = pointMatcher.group(1);
        String yStr = pointMatcher.group(2);
        return new Point(Double.parseDouble(xStr), Double.parseDouble(yStr));
      } else {
        LOG.debug("can't parse {} to Point", pointStr);
        return null;
      }
    }
  }

  public static Point parse(String p) {
    return parser.parse(p);
  }
}
