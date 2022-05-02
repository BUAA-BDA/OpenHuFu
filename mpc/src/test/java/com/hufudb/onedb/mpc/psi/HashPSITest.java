package com.hufudb.onedb.mpc.psi;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.codec.HashFunction;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.random.BasicRandom;
import com.hufudb.onedb.mpc.random.OneDBRandom;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.grpc.OneDBRpcManager;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
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
public class HashPSITest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static OneDBRandom rand = new BasicRandom();

  public List<byte[]> generateData(int size, int rowNum) {
    ImmutableList.Builder<byte[]> randomData = ImmutableList.builder();
    // randomData.add(OneDBCodec.encodeInt(1024));
    // randomData.add(OneDBCodec.encodeInt(2));
    // randomData.add(OneDBCodec.encodeInt(2));
    // for (int i = 3; i < rowNum; ++i) {
    //   randomData.add(rand.randomBytes(size));
    // }
    // return randomData.build();
    for (int i = 0; i < rowNum; ++i) {
      randomData.add(OneDBCodec.encodeInt(i));
    }
    return randomData.build();
  }

  @Test
  public void testHashPSI() throws Exception {
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
      HashPSI psi0 = new HashPSI(rpc0);
      HashPSI psi1 = new HashPSI(rpc1);
      List<byte[]> data0 = generateData(8, 1000);
      List<byte[]> data1 = generateData(8, 10000);
      ExecutorService service = Executors.newFixedThreadPool(2);
      DataPacketHeader header = new DataPacketHeader(0, psi0.getProtocolType().getId(), 0, HashFunction.MD5.getId(), 0, 1);
      Future<List<byte[]>> psiRes0 = service.submit(new Callable<List<byte[]>>() {
        @Override
        public List<byte[]> call() throws Exception {
          return psi0.run(DataPacket.fromByteArrayList(header, data0));
        }
      });
      Future<List<byte[]>> psiRes1 = service.submit(new Callable<List<byte[]>>() {
        @Override
        public List<byte[]> call() throws Exception {
          return psi1.run(DataPacket.fromByteArrayList(header, data1));
        }
      });
      List<byte[]> expect0 = new ArrayList<>();
      List<byte[]> expect1 = new ArrayList<>();
      for (int i = 0; i < data0.size(); ++i) {
        byte[] d0 = data0.get(i);
        for (int j = 0; j < data1.size(); ++j) {
          byte[] d1 = data1.get(j);
          if (Arrays.equals(d0, d1)) {
            expect0.add(OneDBCodec.encodeInt(i));
            expect1.add(OneDBCodec.encodeInt(j));
          }
        }
      }
      System.err.println(expect0.size());
      System.err.println(expect1.size());
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
      rpc0.disconnect();
      rpc1.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
