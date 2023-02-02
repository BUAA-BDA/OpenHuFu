package com.hufudb.openhufu.rpc.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.rpc.Party;
import com.hufudb.openhufu.rpc.utils.DataPacket;
import com.hufudb.openhufu.rpc.utils.DataPacketHeader;
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
public class FQRpcManagerTest {
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  static Random rand = new Random();

  List<byte[]> generatePayloads(int n, int size) {
    List<byte[]> payloads = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      byte[] payload = new byte[size];
      for (int j = 0; j < size; ++j) {
        payload[j] = (byte)rand.nextInt();
      }
      payloads.add(payload);
    }
    return payloads;
  }

  DataPacket generateDataPacket(int senderId, int receiverId) {
    DataPacketHeader header = new DataPacketHeader(1, 2, 3, senderId, receiverId);
    List<byte[]> payloads = generatePayloads(2, 10);
    return DataPacket.fromByteArrayList(header, payloads);
  }

  @Test
  public void OneDBRpcTest() throws Exception {
    String ownerName0 = InProcessServerBuilder.generateName();
    String ownerName1 = InProcessServerBuilder.generateName();
    Party owner0 = new FQOwnerInfo(0, ownerName0);
    Party owner1 = new FQOwnerInfo(1, ownerName1);
    Party owner2 = new FQOwnerInfo(2, "fakeName");
    List<Party> parties = ImmutableList.of(
      owner0, owner1
    );
    List<Channel> channels = Arrays.asList(
      grpcCleanup.register(InProcessChannelBuilder.forName(ownerName0).directExecutor().build()),
      grpcCleanup.register(InProcessChannelBuilder.forName(ownerName1).directExecutor().build())
    );
    FQRpcManager manager = new FQRpcManager(parties, channels);
    assertEquals(2, manager.getPartyNum());
    Set<Party> party = manager.getPartySet();
    assertTrue(party.contains(owner0));
    assertTrue(party.contains(owner1));
    FQRpc rpc0 = (FQRpc) manager.getRpc(0);
    assertFalse("Error when adding an existing party", rpc0.addParty(owner1));
    assertFalse("Error when removing an unexisting party", rpc0.removeParty(owner2));
    FQRpc rpc1 = (FQRpc) manager.getRpc(1);
    Server server0 = InProcessServerBuilder.forName(ownerName0).directExecutor().addService(rpc0.getgRpcService()).build().start();
    Server server1 = InProcessServerBuilder.forName(ownerName1).directExecutor().addService(rpc1.getgRpcService()).build().start();
    grpcCleanup.register(server0);
    grpcCleanup.register(server1);
    rpc0.connect();
    rpc1.connect();
    DataPacket packet0 = generateDataPacket(0, 1);
    DataPacket packet1 = generateDataPacket(1, 0);
    DataPacket packet00 = generateDataPacket(0, 0);
    DataPacket packet11 = generateDataPacket(1, 1);
    DataPacket fakePacket1 = generateDataPacket(1, 2);
    DataPacket fakePacket2 = generateDataPacket(2, 1);
    rpc0.send(packet0);
    rpc1.send(packet1);
    DataPacketHeader headerfrom0 = packet0.getHeader();
    DataPacketHeader headerfrom1 = packet1.getHeader();
    DataPacket r0 = rpc0.receive(headerfrom1);
    DataPacket r1 = rpc1.receive(headerfrom0);
    rpc0.send(packet00);
    rpc1.send(packet11);
    DataPacketHeader headerfrom00 = packet00.getHeader();
    DataPacketHeader headerfrom11 = packet11.getHeader();
    DataPacket r00 = rpc0.receive(headerfrom00);
    DataPacket r11 = rpc1.receive(headerfrom11);
    assertTrue("rpc0 receive wrong message", r0.equals(packet1));
    assertTrue("rpc0 receive wrong message", r1.equals(packet0));
    assertTrue("rpc0 receive wrong message", r00.equals(packet00));
    assertTrue("rpc0 receive wrong message", r11.equals(packet11));
    rpc1.send(fakePacket1);
    rpc0.send(fakePacket2);
    assertEquals(2, rpc0.getSendDataPacketNum(false));
    assertEquals(2, rpc1.getSendDataPacketNum(true));
    assertEquals(40, rpc0.getPayloadByteLength(false));
    assertEquals(40, rpc1.getPayloadByteLength(true));
    rpc0.removeParty(owner1);
    rpc1.removeParty(owner0);
    rpc0.disconnect();
    rpc1.disconnect();
    rpc0.disconnect();
  }
}
