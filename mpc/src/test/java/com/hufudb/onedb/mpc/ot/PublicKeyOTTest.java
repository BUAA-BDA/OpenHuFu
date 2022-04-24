package com.hufudb.onedb.mpc.ot;

import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolType;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import io.grpc.Channel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

@RunWith(JUnit4.class)
public class PublicKeyOTTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  public static OneDBRandom rand = new BasicRandom();

  public RpcManager getRpcManager() throws IOException {
    String ownerName0 = InProcessServerBuilder.generateName();
    String ownerName1 = InProcessServerBuilder.generateName();
    System.out.print("");
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
    InProcessServerBuilder.forName(ownerName0).directExecutor().addService(((OneDBRpc) manager.getRpc(0)).getgRpcService()).build().start();
    InProcessServerBuilder.forName(ownerName1).directExecutor().addService(((OneDBRpc) manager.getRpc(1)).getgRpcService()).build().start();
    return manager;
  }

  public DataPacket generateInitPacket4Sender(int sender, int receiver, List<String> secrets) {
    DataPacketHeader header = new DataPacketHeader(0, ProtocolType.PK_OT.getId(), 0, secrets.size(), sender, receiver);
    List<byte[]> payloads = secrets.stream().map(s -> s.getBytes()).collect(Collectors.toList());
    return DataPacket.fromByteArrayList(header, payloads);
  }

  public DataPacket generateInitPacket4Receiver(int sender, int receiver, int select) {
    DataPacketHeader header = new DataPacketHeader(0, ProtocolType.PK_OT.getId(), 0, select, sender, receiver);
    return DataPacket.fromByteArrayList(header, ImmutableList.of());
  }

  @Ignore
  @Test
  public void testPublicKeyOT() throws IOException, InterruptedException {
    RpcManager managers = getRpcManager();
    Rpc m0 = managers.getRpc(0);
    Rpc m1 = managers.getRpc(1);
    PublicKeyOT otSender = new PublicKeyOT(m0);
    PublicKeyOT otReceiver = new PublicKeyOT(m1);
    List<String> secrets = Arrays.asList("Alice", "Bob", "Jerry", "Tom");
    int tid = rand.nextInt() % secrets.size();
    String expect = secrets.get(tid);
    otSender.run(generateInitPacket4Sender(1, 0, secrets));
    DataPacketHeader resultHeader = otReceiver.run(generateInitPacket4Receiver(1, 0, tid));
    Thread.sleep(1);
    DataPacket resultPacket = otReceiver.getResult(resultHeader);
    String actual = new String(resultPacket.getPayload().get(0));
    assertTrue("res", expect.equals(actual));
  }
}
