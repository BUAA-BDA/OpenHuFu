package com.hufudb.onedb.mpc.codec;

import java.nio.ByteBuffer;

public class OneDBCodec {
  static byte TRUE = 1;
  static byte FALSE = 0;

  private OneDBCodec() {}

  public static byte[] encodeInt(int value) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
  }

  public static int decodeInt(byte[] value) {
    return ByteBuffer.wrap(value).getInt();
  }

  public static byte[] encodeLong(long value) {
    return ByteBuffer.allocate(Integer.BYTES).putLong(value).array();
  }

  public static byte[] encodeString(String str) {
    return str.getBytes();
  }

  public static long decodeLong(byte[] value) {
    return ByteBuffer.wrap(value).getLong();
  }

  public static byte[] encodeBoolean(boolean value) {
    byte v = value ? TRUE : FALSE;
    return ByteBuffer.allocate(1).put(v).array();
  }

  public static boolean decodeBoolean(byte[] value) {
    return value[0] == TRUE;
  }

  public static String decodeString(byte[] value) {
    return new String(value);
  }

  // a = a ^ b
  public static void xor(byte[] a, byte[] b) {
    assert a.length == b.length;
    for (int i = 0; i < a.length; ++i) {
      a[i] = (byte) (a[i] ^ b[i]);
    }
  }
}
