package com.hufudb.onedb.mpc.codec;

import java.nio.ByteBuffer;

public class OneDBCodec {
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

  public static long decodeLong(byte[] value) {
    return ByteBuffer.wrap(value).getLong();
  }
}
