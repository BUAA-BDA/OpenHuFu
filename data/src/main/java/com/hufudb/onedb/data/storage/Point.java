package com.hufudb.onedb.data.storage;

import java.nio.ByteBuffer;

public class Point {
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
}
