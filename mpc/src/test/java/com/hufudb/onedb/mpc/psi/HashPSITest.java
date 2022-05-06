package com.hufudb.onedb.mpc.psi;

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
import com.hufudb.onedb.mpc.codec.HashFunction;
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
public class HashPSITest {
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
  private HashPSI psi0;
  private HashPSI psi1;

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
    psi0 = new HashPSI(rpc0);
    psi1 = new HashPSI(rpc1);
  }

  @After
  public void shutdown() {
    rpc0.disconnect();
    rpc1.disconnect();
  }

  public void runHashPSI(List<byte[]> data0, List<byte[]> data1, HashFunction func, int senderId, int receiverId) throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(2);
    int taskId = 0;
    int hashType = func.getId();
    Future<List<byte[]>> psiRes0 = service.submit(new Callable<List<byte[]>>() {
      @Override
      public List<byte[]> call() throws Exception {
        return psi0.run(taskId, ImmutableList.of(senderId, receiverId), data0, hashType);
      }
    });
    Future<List<byte[]>> psiRes1 = service.submit(new Callable<List<byte[]>>() {
      @Override
      public List<byte[]> call() throws Exception {
        return psi1.run(taskId, ImmutableList.of(senderId, receiverId), data1, hashType);
      }
    });
    List<byte[]> expect0 = new ArrayList<>();
    List<byte[]> expect1 = new ArrayList<>();
    for (int i = 0; i < data1.size(); ++i) {
      byte[] d1 = data1.get(i);
      for (int j = 0; j < data0.size(); ++j) {
        byte[] d0 = data0.get(j);
        if (Arrays.equals(d0, d1)) {
          expect0.add(OneDBCodec.encodeInt(j));
          expect1.add(OneDBCodec.encodeInt(i));
        }
      }
    }
    List<byte[]> actual0 = psiRes0.get();
    List<byte[]> actual1 = psiRes1.get();
    assertEquals(expect0.size(), actual0.size());
    assertEquals(expect1.size(), actual1.size());
    for (int i = 0; i < expect0.size(); ++i) {
      assertArrayEquals(expect0.get(i), actual0.get(i));
    }
    for (int i = 0; i < expect1.size(); ++i) {
      assertArrayEquals(expect1.get(i), actual1.get(i));
    }
  }

  @Test
  public void testHashPSIMD5() throws Exception {
    try {
      runHashPSI(generateData(8, 10), generateData(8, 100), HashFunction.MD5, 0, 1);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testHashPSISHA256() throws Exception {
    try {
      runHashPSI(generateData(8, 10), generateData(8, 100), HashFunction.SHA256, 0, 1);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testHashPSIEqaulSize() throws Exception {
    try {
      runHashPSI(generateData(8, 10), generateData(8, 10), HashFunction.MD5, 0, 1);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testHashPSISenderLarger() throws Exception {
    try {
      runHashPSI(generateData(8, 10), generateData(8, 100), HashFunction.MD5, 1, 0);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
