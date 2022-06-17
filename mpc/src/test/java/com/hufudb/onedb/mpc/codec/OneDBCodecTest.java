package com.hufudb.onedb.mpc.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class OneDBCodecTest {

  @Test
  public void boolTest() {
    boolean t = true;
    byte[] tc = OneDBCodec.encodeBoolean(t);
    assertTrue(OneDBCodec.decodeBoolean(tc));
    boolean f = false;
    byte[] fc = OneDBCodec.encodeBoolean(f);
    assertFalse(OneDBCodec.decodeBoolean(fc));
  }

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

  @Test
  public void floatTest() {
    float a = 3.14159F;
    byte[] ac = OneDBCodec.encodeFloat(a);
    assertEquals(a, OneDBCodec.decodeFloat(ac), 0.0000001);
    float b = 2.71828F;
    byte[] bc = OneDBCodec.encodeFloat(b);
    assertEquals(b, OneDBCodec.decodeFloat(bc), 0.0000001);
    float c = 21347612657.7F;
    byte[] cc = OneDBCodec.encodeFloat(c);
    assertEquals(c, OneDBCodec.decodeFloat(cc), 0.1);
  }

  @Test
  public void doubleTest() {
    double a = 3.14159;
    byte[] ac = OneDBCodec.encodeDouble(a);
    assertEquals(a, OneDBCodec.decodeDouble(ac), 0.0000001);
    double b = 2.71828;
    byte[] bc = OneDBCodec.encodeDouble(b);
    assertEquals(b, OneDBCodec.decodeDouble(bc), 0.0000001);
    double c = 21347612657.7;
    byte[] cc = OneDBCodec.encodeDouble(c);
    assertEquals(c, OneDBCodec.decodeDouble(cc), 0.001);
  }
}
