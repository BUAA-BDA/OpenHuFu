package com.hufudb.openhufu.data.storage;

import static org.junit.Assert.assertEquals;

import com.hufudb.openhufu.data.storage.Point;
import java.util.Random;
import org.junit.Test;

public class PointTest {
  @Test
  public void testPoint() {
    Random random = new Random();
    for (int i = 0; i < 10; ++i) {
      double x = random.nextDouble();
      double y = random.nextDouble();
      Point p = new Point(x, y);
      assertEquals(x, p.getX(), 0.0001);
      assertEquals(y, p.getY(), 0.0001);
      byte[] bs = p.toBytes();
      Point q = Point.fromBytes(bs);
      assertEquals(x, q.getX(), 0.0001);
      assertEquals(y, q.getY(), 0.0001);
    }
    Point p = new Point(0, 0);
    assertEquals("POINT(0.000000 0.000000)", p.toString());
  }
}
