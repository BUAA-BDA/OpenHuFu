package com.hufudb.onedb.data.storage;

import java.util.ArrayList;
import java.util.List;

public class BitArray {
  final private int capacity;
  private byte[] bits;

  BitArray(int capacity, byte[] bits) {
    this.capacity = capacity;
    this.bits = bits;
  }

  public BitArray(byte[] bits) {
    this(bits.length * Byte.SIZE, bits);
  }

  public BitArray(int capacity) {
    this(capacity, new byte[(capacity + 7) >> 3]);
  }

  public static BitArray valueOf(byte[] bits) {
    return new BitArray(bits);
  }

  public void set(int i) {
    bits[i >> 3] |= 1 << (i & 0x7);
  }

  public void clear(int i) {
    bits[i >> 3] &= ~(1 << (i & 0x7));
  }

  public void set(int i, boolean value) {
    if (value) {
      set(i);
    } else {
      clear(i);
    }
  }

  public boolean get(int i) {
    return (bits[i >> 3] & (1 << (i & 0x7))) != 0;
  }

  public int size() {
    return capacity;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * return [from, to)
   */
  public BitArray get(int from, int to) {
    final int length = (to - from + 7) >> 3;
    final int tail = (to - from) & 0x7;
    BitArray ans = new BitArray(to - from);
    int offset = from & 0x7;
    int begin = from >> 3;
    boolean aligned = offset == 0;
    for (int i = 0; i < length - 1; ++i) {
      ans.bits[i] = aligned ? bits[begin + i]
          : (byte) (((bits[begin + i] & 0xFF) >>> offset) | (bits[begin + i + 1] << (8 - offset)));
    }
    int end = (to - 1) >> 3;
    ans.bits[length - 1] |= ((bits[begin + length - 1] & 0xFF) >>> offset);
    if (end > begin + length - 1) {
      ans.bits[length - 1] |= (bits[begin + length] << (8 - offset));
    }
    if (tail != 0) {
      ans.bits[length - 1] &= (0xFF >>> (8 - tail));
    }
    return ans;
  }

  public byte[] toByteArray() {
    return bits;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < capacity; ++i) {
      buffer.append(get(i) ? '1' : '0');
    }
    return buffer.toString();
  }

  public static class Builder {
    List<Byte> bytes;
    byte cur;
    int count;

    Builder() {
      bytes = new ArrayList<>();
      cur = 0;
      count = 0;
    }

    public void add(boolean value) {
      if (value) {
        cur |= 1 << (count & 0x7);
      }
      count++;
      if ((count & 0x7) == 0) {
        bytes.add(cur);
        cur = 0;
      }
    }

    public void clear() {
      bytes.clear();
      cur = 0;
      count = 0;
    }

    public BitArray build() {
      byte[] array = buildByteArray();
      return new BitArray(count, array);
    }

    public byte[] buildByteArray() {
      if ((count & 0x7) != 0) {
        bytes.add(cur);
      }
      final int len = bytes.size();
      byte[] array = new byte[len];
      for (int i = 0; i < len; ++i) {
        array[i] = bytes.get(i);
      }
      return array;
    }
  }
}
