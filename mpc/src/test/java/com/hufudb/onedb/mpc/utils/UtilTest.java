package com.hufudb.onedb.mpc.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.random.BasicRandom;
import com.hufudb.onedb.mpc.random.OneDBRandom;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.grpc.OneDBRpcManager;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import io.grpc.Channel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

@RunWith(JUnit4.class)
public class UtilTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  public static OneDBRandom rand = new BasicRandom();

  public List<byte[]> generateData(int size, int rowNum) {
    ImmutableList.Builder<byte[]> randomData = ImmutableList.builder();
    for (int i = 0; i < 5; ++i) {
      // create duplicate elements
      randomData.add(OneDBCodec.encodeInt(i));
      randomData.add(OneDBCodec.encodeInt(i));
    }
    for (int i = 0; i < rowNum; ++i) {
      randomData.add(OneDBCodec.encodeInt(i));
      randomData.add(OneDBCodec.encodeInt(i));
    }
    return randomData.build();
  }

  private OneDBRpc rpc0;
  private OneDBRpc rpc1;

  @Before
  public void setUp() throws IOException {
    String ownerName0 = InProcessServerBuilder.generateName();
    String ownerName1 = InProcessServerBuilder.generateName();
    Party owner0 = new OneDBOwnerInfo(0, ownerName0);
    Party owner1 = new OneDBOwnerInfo(1, ownerName1);
    List<Party> parties = ImmutableList.of(owner0, owner1);
    List<Channel> channels = Arrays.asList(
        grpcCleanup.register(InProcessChannelBuilder.forName(ownerName0).directExecutor().build()),
        grpcCleanup.register(InProcessChannelBuilder.forName(ownerName1).directExecutor().build()));
    OneDBRpcManager manager = new OneDBRpcManager(parties, channels);
    rpc0 = (OneDBRpc) manager.getRpc(0);
    rpc1 = (OneDBRpc) manager.getRpc(1);
    grpcCleanup.register(InProcessServerBuilder.forName(ownerName0).directExecutor()
        .addService(rpc0.getgRpcService()).build().start());
    grpcCleanup.register(InProcessServerBuilder.forName(ownerName1).directExecutor()
        .addService(rpc1.getgRpcService()).build().start());
    rpc0.connect();
    rpc1.connect();
  }

  @After
  public void shutdown() {
    rpc0.disconnect();
    rpc1.disconnect();
  }

  List<byte[]> generateRandomBytes(int rowSize, int rowNum) {
    List<byte[]> res = new ArrayList<>();
    for (int i = 0; i < rowNum; ++i) {
      res.add(rand.randomBytes(rowSize));
    }
    return res;
  }

  void runStream(List<byte[]> payloads, Stream s0, Stream s1, int senderId, int receiverId)
      throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(2);
    long taskId = 0;
    Future<Object> res0 = service.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return s0.run(taskId, ImmutableList.of(s1.getOwnId()), payloads, s0.getOwnId());
      }
    });
    Future<Object> res1 = service.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return s1.run(taskId, ImmutableList.of(s1.getOwnId()), ImmutableList.of(), s0.getOwnId());
      }
    });

    List<byte[]> actual = (List<byte[]>) res1.get();
    assertEquals(payloads.size(), actual.size());
    for (int i = 0; i < payloads.size(); ++i) {
      assertArrayEquals(payloads.get(i), actual.get(i));
    }
  }

  void runBoardcast(List<byte[]> payloads, Boardcast b0, Boardcast b1, int senderId, int receiverId)
      throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(2);
    long taskId = 0;
    Future<Object> res0 = service.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return b0.run(taskId, ImmutableList.of(b1.getOwnId()), payloads, b0.getOwnId(), 1);
      }
    });
    Future<Object> res1 = service.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return b1.run(taskId, ImmutableList.of(b1.getOwnId()), ImmutableList.of(), b0.getOwnId(), 1);
      }
    });

    List<byte[]> actual = (List<byte[]>) res1.get();
    assertEquals(payloads.size(), actual.size());
    for (int i = 0; i < payloads.size(); ++i) {
      assertArrayEquals(payloads.get(i), actual.get(i));
    }
  }

  @Test
  public void testStream() throws Exception {
    runStream(generateRandomBytes(8, 32), new Stream(rpc0, 50), new Stream(rpc1, 50), 0, 1);
    runStream(generateRandomBytes(8, 32), new Stream(rpc0), new Stream(rpc1), 1, 0);
  }

  @Test
  public void testStreamMultiple() throws Exception {
    runStream(generateRandomBytes(10, 10), new Stream(rpc0, 50), new Stream(rpc1, 50), 0, 1);
    runStream(generateRandomBytes(10, 10), new Stream(rpc0, 50), new Stream(rpc1, 50), 1, 0);
  }

  @Test
  public void testBoardcast() throws Exception {
    runBoardcast(generateRandomBytes(10, 10), new Boardcast(rpc0), new Boardcast(rpc1), 0, 1);
    runBoardcast(generateRandomBytes(10, 10), new Boardcast(rpc0), new Boardcast(rpc1), 1, 0);
    runBoardcast(generateRandomBytes(10, 10), new Boardcast(rpc0), new Boardcast(rpc1), 1, 0);

  }
}
