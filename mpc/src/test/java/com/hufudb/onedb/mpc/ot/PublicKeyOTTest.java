package com.hufudb.onedb.mpc.ot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.random.BasicRandom;
import com.hufudb.onedb.mpc.random.OneDBRandom;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.RpcManager;
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
public class PublicKeyOTTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static OneDBRandom rand = new BasicRandom();

  public DataPacket generateInitPacket4Sender(int sender, int receiver, List<String> secrets) {
    DataPacketHeader header = new DataPacketHeader(0, ProtocolType.PK_OT.getId(), 0, secrets.size(), sender, receiver);
    List<byte[]> payloads = secrets.stream().map(s -> s.getBytes()).collect(Collectors.toList());
    return DataPacket.fromByteArrayList(header, payloads);
  }

  public DataPacket generateInitPacket4Receiver(int sender, int receiver, int select) {
    DataPacketHeader header = new DataPacketHeader(0, ProtocolType.PK_OT.getId(), 0, 2, sender, receiver);
    return DataPacket.fromByteArrayList(header, ImmutableList.of(OneDBCodec.encodeInt(select)));
  }

  @Test
  public void testPublicKeyOT() throws Exception {
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
      PublicKeyOT otSender = new PublicKeyOT(rpc0);
      PublicKeyOT otReceiver = new PublicKeyOT(rpc1);
      List<String> secrets = Arrays.asList("Alice", "Bob", "Jerry", "Tom");
      int tid = rand.nextInt(secrets.size());
      String expect = secrets.get(tid);
      ExecutorService service = Executors.newFixedThreadPool(2);
      Future<DataPacket> senderRes = service.submit(
        new Callable<DataPacket>() {
          @Override
          public DataPacket call() throws Exception {
            return otSender.run(generateInitPacket4Sender(0, 1, secrets));
          }
        }
      );
      Future<DataPacket> receiverRes = service.submit(
        new Callable<DataPacket>() {
        @Override
        public DataPacket call() throws Exception {
          return otReceiver.run(generateInitPacket4Receiver(0, 1, tid));
        }
      });
      DataPacket result = receiverRes.get();
      String actual = new String(result.getPayload().get(0));
      assertEquals(expect, actual);
      rpc0.disconnect();
      rpc1.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
