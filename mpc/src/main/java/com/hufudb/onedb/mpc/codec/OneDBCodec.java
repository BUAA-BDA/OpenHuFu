package com.hufudb.onedb.mpc.codec;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class OneDBCodec {
  static byte TRUE = 1;
  static byte FALSE = 0;

  private OneDBCodec() {
  }

  public static byte[] encodeInt(int value) {
    return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
  }

  public static int decodeInt(byte[] value) {
    return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

  public static byte[] encodeLong(long value) {
    return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
  }

  public static long decodeLong(byte[] value) {
    return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
  }

  public static byte[] encodeFloat(float value) {
    return ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
  }

  public static float decodeFloat(byte[] value) {
    return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getFloat();
  }

  public static byte[] encodeDouble(double value) {
    return ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();
  }

  public static double decodeDouble(byte[] value) {
    return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getDouble();
  }

  public static byte[] encodeBoolean(boolean value) {
    byte v = value ? TRUE : FALSE;
    return ByteBuffer.allocate(1).put(v).array();
  }

  public static boolean decodeBoolean(byte[] value) {
    return value[0] == TRUE;
  }

  public static byte[] encodeString(String str) {
    return str.getBytes();
  }

  public static String decodeString(byte[] value) {
    return new String(value);
  }

  // a = a ^ b
  public static void xor(byte[] a, byte[] b, byte[] res) {
    for (int i = 0; i < res.length; ++i) {
      res[i] = (byte) ((i >= a.length ? 0 : a[i]) ^ (i >= b.length ? 0 : b[i]));
    }
  }

  public static List<BigInteger> decodeBigInteger(byte[] value, int groupSize, byte precursor) {
    List<BigInteger> bigIntegerList = new ArrayList<>();
    int loopCnt = value.length % groupSize == 0 ? value.length / groupSize : value.length / groupSize + 1;
    for (int i = 0; i < loopCnt; i++) {
      byte[] byteList = new byte[Math.min(groupSize, value.length - i * groupSize) + 1];
      byteList[0] = precursor;
      int m = 1;
      for (int j = i * groupSize; j < Math.min(value.length, (i + 1) * groupSize); j++) {
        byteList[m++] = value[j];
      }
      bigIntegerList.add(new BigInteger(byteList));
    }
    return bigIntegerList;
  }

  public static byte[] encodeOriginalData(List<BigInteger> value, int groupSize) {
    byte[] lastByteArray = value.get(value.size() - 1).toByteArray();
    assert lastByteArray[0] == 1;
    byte[] originalData = new byte[groupSize * (value.size() - 1) + lastByteArray.length - 1];
    int m = 0;
    for (int i = 0; i < value.size() - 1; i++) {
      byte[] curByteArray = value.get(i).toByteArray();
      assert curByteArray[0] == 1 && curByteArray.length == groupSize + 1;
      for (int j = 1; j < curByteArray.length; j++) {
        originalData[m++] = curByteArray[j];
      }
    }
    for (int j = 1; j < lastByteArray.length; j++) {
      originalData[m++] = lastByteArray[j];
    }
    return originalData;
  }

  public static byte[] encodeBigInteger(List<BigInteger> value, int groupSize) {
    byte[] byteList = new byte[groupSize * value.size()];
    int m = 0;
    for (BigInteger bigInteger : value) {
      byte[] originByteList = bigInteger.toByteArray();
      assert originByteList.length <= groupSize + 1;
      assert originByteList.length != groupSize + 1 || originByteList[0] == 0;
      for (int i = 0; i < groupSize - originByteList.length; i++) {
        byteList[m++] = 0;
      }
      for (int i = originByteList.length > groupSize ? 1 : 0; i < originByteList.length; i++) {
        byteList[m++] = originByteList[i];
      }
    }
    assert m == groupSize * value.size();
    return byteList;
  }
}
