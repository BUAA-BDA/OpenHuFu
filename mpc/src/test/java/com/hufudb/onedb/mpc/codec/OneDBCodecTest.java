package com.hufudb.onedb.mpc.codec;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class OneDBCodecTest {

  @Test
  public void integerTest() {
    int i = 10;
    byte[] ic = OneDBCodec.encodeInt(i);
    assertEquals(10, ic[0]);
    assertEquals(10, OneDBCodec.decodeInt(ic));
    int j = 257;
    byte[] jc = OneDBCodec.encodeInt(j);
    assertEquals(1, jc[0]);
    assertEquals(1, jc[1]);
    assertEquals(257, OneDBCodec.decodeInt(jc));
  }

  @Test
  public void longTest() {
    long i = 10;
    byte[] ic = OneDBCodec.encodeLong(i);
    assertEquals(10, ic[0]);
    assertEquals(10, OneDBCodec.decodeLong(ic));
    long j = 257;
    byte[] jc = OneDBCodec.encodeLong(j);
    assertEquals(1, jc[0]);
    assertEquals(1, jc[1]);
    assertEquals(257, OneDBCodec.decodeLong(jc));
  }
}
