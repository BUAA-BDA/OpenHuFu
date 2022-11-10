package com.hufudb.onedb.data.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Test;

public class BitArrayTest {
  @Test
  public void bitArrayBuilderTest() {
    BitArray.Builder builder = BitArray.builder();
    List<Boolean> expect = new ArrayList<>();
    Random random = new Random();
    final int SIZE = 201;
    for (int i = 0; i < SIZE; ++i) {
      boolean v = random.nextBoolean();
      expect.add(v);
      builder.add(v);
    }
    BitArray bits = builder.build();
    assertEquals(expect.size(), bits.size());
    for (int i = 0; i < SIZE; ++i) {
      assertEquals(expect.get(i), bits.get(i));
    }
    builder.clear();
    BitArray empty = builder.build();
    assertEquals(0, empty.size());
  }

  @Test
  public void bitArrayTest() {
    BitArray b = new BitArray(33);
    b.set(32, true);
    b.set(30, true);
    b.set(9, true);
    assertTrue(b.get(32));
    assertTrue(b.get(30));
    assertTrue(b.get(9));
    assertFalse(b.get(10));
    assertFalse(b.get(8));
    assertFalse(b.get(31));
    assertFalse(b.get(29));
    b.set(9, false);
    assertTrue(b.get(32));
    assertFalse(b.get(31));
    assertTrue(b.get(30));
    assertFalse(b.get(9));
    b.set(9, true);
    assertTrue(b.get(9));
    BitArray c = b.get(15, 33);
    assertTrue(c.get(17));
    assertFalse(c.get(16));
    assertTrue(c.get(15));
    assertFalse(c.get(0));
    BitArray d = new BitArray(65);
    d.set(7, false);
    d.set(27, true);
    d.set(46, true);
    d.set(65, true);
    d.set(64, true);
    BitArray e = d.get(46, 65);
    assertTrue(e.get(0));
    assertFalse(e.get(5));
    assertFalse(e.get(6));
    assertTrue(e.get(18));
    assertFalse(e.get(19));
    BitArray f = d.get(7, 28);
    assertFalse(f.get(0));
    assertTrue(f.get(20));
    BitArray g = new BitArray(16);
    g.set(7, true);
    g.set(8, true);
    g.set(10, true);
    BitArray h = g.get(6, 10);
    assertFalse(h.get(0));
    assertTrue(h.get(1));
    assertTrue(h.get(2));
    assertFalse(h.get(3));
    assertFalse(h.get(4));
  }
}
