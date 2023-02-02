package com.hufudb.openhufu.mpc.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class OpenHuFuCodecTest {

  @Test
  public void boolTest() {
    boolean t = true;
    byte[] tc = OpenHuFuCodec.encodeBoolean(t);
    assertTrue(OpenHuFuCodec.decodeBoolean(tc));
    boolean f = false;
    byte[] fc = OpenHuFuCodec.encodeBoolean(f);
    assertFalse(OpenHuFuCodec.decodeBoolean(fc));
  }

  @Test
  public void integerTest() {
    int i = 10;
    byte[] ic = OpenHuFuCodec.encodeInt(i);
    assertEquals(10, ic[0]);
    assertEquals(10, OpenHuFuCodec.decodeInt(ic));
    int j = 257;
    byte[] jc = OpenHuFuCodec.encodeInt(j);
    assertEquals(1, jc[0]);
    assertEquals(1, jc[1]);
    assertEquals(257, OpenHuFuCodec.decodeInt(jc));
  }

  @Test
  public void longTest() {
    long i = 10;
    byte[] ic = OpenHuFuCodec.encodeLong(i);
    assertEquals(10, ic[0]);
    assertEquals(10, OpenHuFuCodec.decodeLong(ic));
    long j = 257;
    byte[] jc = OpenHuFuCodec.encodeLong(j);
    assertEquals(1, jc[0]);
    assertEquals(1, jc[1]);
    assertEquals(257, OpenHuFuCodec.decodeLong(jc));
  }

  @Test
  public void floatTest() {
    float a = 3.14159F;
    byte[] ac = OpenHuFuCodec.encodeFloat(a);
    assertEquals(a, OpenHuFuCodec.decodeFloat(ac), 0.0000001);
    float b = 2.71828F;
    byte[] bc = OpenHuFuCodec.encodeFloat(b);
    assertEquals(b, OpenHuFuCodec.decodeFloat(bc), 0.0000001);
    float c = 21347612657.7F;
    byte[] cc = OpenHuFuCodec.encodeFloat(c);
    assertEquals(c, OpenHuFuCodec.decodeFloat(cc), 0.1);
  }

  @Test
  public void doubleTest() {
    double a = 3.14159;
    byte[] ac = OpenHuFuCodec.encodeDouble(a);
    assertEquals(a, OpenHuFuCodec.decodeDouble(ac), 0.0000001);
    double b = 2.71828;
    byte[] bc = OpenHuFuCodec.encodeDouble(b);
    assertEquals(b, OpenHuFuCodec.decodeDouble(bc), 0.0000001);
    double c = 21347612657.7;
    byte[] cc = OpenHuFuCodec.encodeDouble(c);
    assertEquals(c, OpenHuFuCodec.decodeDouble(cc), 0.001);
  }
}
