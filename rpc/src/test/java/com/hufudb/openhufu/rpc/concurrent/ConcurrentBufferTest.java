package com.hufudb.openhufu.rpc.concurrent;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Test;

public class ConcurrentBufferTest {
  @Test
  public void bufferSmokeTest() throws Exception {
    // create a small buffer with 8 slot
    ConcurrentBuffer<Integer, Integer> buffer = new ConcurrentBuffer<>(3);
    ExecutorService threadpool = Executors.newFixedThreadPool(4);
    List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < 4; ++i) {
      final int id = i * 2;
      tasks.add(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            return buffer.put(id, id) && buffer.put(id + 1, id + 1);
          }
      });
    }
    List<Future<Boolean>> futures = threadpool.invokeAll(tasks);
    for (Future<Boolean> future : futures) {
      assertTrue(future.get());
    }
    assertFalse(buffer.put(100, 100));
    assertTrue(buffer.pop(1) == 1);
    assertNull(buffer.pop(100));
  }
}
