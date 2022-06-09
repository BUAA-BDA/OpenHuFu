package com.hufudb.onedb.mpc.gmw;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.bristol.CircuitType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.ot.PublicKeyOT;
import com.hufudb.onedb.mpc.random.BasicRandom;
import com.hufudb.onedb.mpc.random.OneDBRandom;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import com.hufudb.onedb.rpc.grpc.OneDBRpcManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

@RunWith(JUnit4.class)
public class GMWTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static OneDBRandom rand = new BasicRandom();

  List<byte[]> encodeValue(int value) {
    return ImmutableList.of(OneDBCodec.encodeInt(value));
  }

  public void testcase(int a, int b) throws Exception {
    try {
      String ownerName0 = InProcessServerBuilder.generateName();
      String ownerName1 = InProcessServerBuilder.generateName();
      Party owner0 = new OneDBOwnerInfo(0, ownerName0);
      Party owner1 = new OneDBOwnerInfo(1, ownerName1);
      List<Party> parties = ImmutableList.of(
        owner0, owner1
      );
      List<Channel> channels = Arrays.asList(
        grpcCleanup.register(InProcessChannelBuilder.forName(ownerName0).directExecutor().build()),
        grpcCleanup.register(InProcessChannelBuilder.forName(ownerName1).directExecutor().build())
      );
      OneDBRpcManager manager = new OneDBRpcManager(parties, channels);
      OneDBRpc rpc0 = (OneDBRpc) manager.getRpc(0);
      OneDBRpc rpc1 = (OneDBRpc) manager.getRpc(1);
      Server server0 = InProcessServerBuilder.forName(ownerName0).directExecutor().addService(rpc0.getgRpcService()).build().start();
      Server server1 = InProcessServerBuilder.forName(ownerName1).directExecutor().addService(rpc1.getgRpcService()).build().start();
      grpcCleanup.register(server0);
      grpcCleanup.register(server1);
      rpc0.connect();
      rpc1.connect();
      ExecutorService threadPool0 = Executors.newFixedThreadPool(4);
      ExecutorService threadPool1 = Executors.newFixedThreadPool(4);
      PublicKeyOT otSender = new PublicKeyOT(rpc0);
      PublicKeyOT otReceiver = new PublicKeyOT(rpc1);
      GMW gmwSender = new GMW(rpc0, otSender, threadPool0);
      GMW gmwReceiver = new GMW(rpc1, otReceiver, threadPool1);
      ExecutorService service = Executors.newFixedThreadPool(2);
      Future<List<byte[]>> senFuture = service.submit(
        new Callable<List<byte[]>>() {
          @Override
          public List<byte[]> call() throws Exception {
            return gmwSender.run(0, ImmutableList.of(0, 1), encodeValue(a), CircuitType.ADD_32.getId());
          }
        }
      );
      Future<List<byte[]>> recFuture = service.submit(
        new Callable<List<byte[]>>() {
        @Override
        public List<byte[]> call() throws Exception {
          return gmwReceiver.run(0, ImmutableList.of(0, 1), encodeValue(b), CircuitType.ADD_32.getId());
        }
      });
      byte[] senRes = senFuture.get().get(0);
      byte[] recRes = recFuture.get().get(0);
      byte[] res = new byte[5];
      OneDBCodec.xor(senRes, recRes, res);
      int actual = OneDBCodec.decodeInt(res);
      int expect = a + b;
      assertEquals(expect, actual);
      rpc0.disconnect();
      rpc1.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testGMW() throws Exception {
    testcase(1, 1);
  }
}
