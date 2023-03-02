package com.hufudb.openhufu.mpc.aby;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.mpc.codec.OpenHuFuCodec;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AbyTest
{

  @Test
  public void testAby() throws InterruptedException, ExecutionException {
    final Aby aby0 = new Aby(0, "127.0.0.1", 7766);
    final Aby aby1 = new Aby(1, "127.0.0.1", 7767);
    Random random = new Random();
    ExecutorService service = Executors.newFixedThreadPool(2);
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10; ++i) {
      final int A = Math.abs(random.nextInt());
      final int B = Math.abs(random.nextInt());
      final boolean expect = A > B;
      Future<Boolean> r0 = service.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return OpenHuFuCodec.decodeBoolean(aby0.run(0, ImmutableList.of(0, 1), ImmutableList.of(OpenHuFuCodec.encodeInt(A)), OperatorType.GT, ColumnType.INT, "127.0.0.1", 7767).get(0));
        }
      });
      Future<Boolean> r1 = service.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return OpenHuFuCodec.decodeBoolean(aby1.run(0, ImmutableList.of(0, 1), ImmutableList.of(OpenHuFuCodec.encodeInt(B)), OperatorType.GT, ColumnType.INT, "127.0.0.1", 7766).get(0));
        }
      });
      assertEquals(expect, r0.get());
      assertEquals(expect, r1.get());
    }
    final int C = 30;
    final int D = 30;
    final boolean expect = C > D;
    Future<Boolean> r0 = service.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return OpenHuFuCodec.decodeBoolean(aby0.run(0, ImmutableList.of(0, 1), ImmutableList.of(OpenHuFuCodec.encodeInt(C)), OperatorType.GT, ColumnType.INT, "127.0.0.1", 7767).get(0));
      }
    });
    Future<Boolean> r1 = service.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return OpenHuFuCodec.decodeBoolean(aby1.run(0, ImmutableList.of(0, 1), ImmutableList.of(OpenHuFuCodec.encodeInt(D)), OperatorType.GT, ColumnType.INT, "127.0.0.1", 7766).get(0));
      }
    });
    assertEquals(expect, r0.get());
    assertEquals(expect, r1.get());
    long end = System.currentTimeMillis();
    System.err.printf("10 loop of millionaire use %d ms\n", end - start);
  }
}
