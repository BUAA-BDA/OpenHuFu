package com.hufudb.openhufu.mpc.secretMultiply;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.mpc.multiply.SecretMultiply;
import com.hufudb.openhufu.rpc.Party;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuOwnerInfo;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpc;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpcManager;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class SecretMultiplyTest {
  public final static GeometryFactory geoFactory = new GeometryFactory();
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  OpenHuFuRpcManager manager;
  ExecutorService threadpool = Executors.newFixedThreadPool(5);

  @Before
  public void setUp() throws IOException {
    String ownerName0 = InProcessServerBuilder.generateName();
    String ownerName1 = InProcessServerBuilder.generateName();
    String ownerName2 = InProcessServerBuilder.generateName();
    String ownerName3 = InProcessServerBuilder.generateName();
    String ownerName4 = InProcessServerBuilder.generateName();
    Party owner0 = new OpenHuFuOwnerInfo(0, ownerName0);
    Party owner1 = new OpenHuFuOwnerInfo(1, ownerName1);
    Party owner2 = new OpenHuFuOwnerInfo(2, ownerName2);
    Party owner3 = new OpenHuFuOwnerInfo(3, ownerName3);
    Party owner4 = new OpenHuFuOwnerInfo(4, ownerName4);
    List<Party> parties = ImmutableList.of(owner0, owner1, owner2, owner3, owner4);
    List<Channel> channels = Arrays.asList(
            grpcCleanup.register(InProcessChannelBuilder.forName(ownerName0).directExecutor().build()),
            grpcCleanup.register(InProcessChannelBuilder.forName(ownerName1).directExecutor().build()),
            grpcCleanup.register(InProcessChannelBuilder.forName(ownerName2).directExecutor().build()),
            grpcCleanup.register(InProcessChannelBuilder.forName(ownerName3).directExecutor().build()),
            grpcCleanup.register(InProcessChannelBuilder.forName(ownerName4).directExecutor().build()));
    manager = new OpenHuFuRpcManager(parties, channels);
    OpenHuFuRpc rpc0 = (OpenHuFuRpc) manager.getRpc(0);
    OpenHuFuRpc rpc1 = (OpenHuFuRpc) manager.getRpc(1);
    OpenHuFuRpc rpc2 = (OpenHuFuRpc) manager.getRpc(2);
    OpenHuFuRpc rpc3 = (OpenHuFuRpc) manager.getRpc(3);
    OpenHuFuRpc rpc4 = (OpenHuFuRpc) manager.getRpc(4);
    rpc0.connect();
    rpc1.connect();
    rpc2.connect();
    rpc3.connect();
    rpc4.connect();
    Server server0 = InProcessServerBuilder.forName(ownerName0).directExecutor()
            .addService(rpc0.getgRpcService()).build().start();
    Server server1 = InProcessServerBuilder.forName(ownerName1).directExecutor()
            .addService(rpc1.getgRpcService()).build().start();
    Server server2 = InProcessServerBuilder.forName(ownerName2).directExecutor()
            .addService(rpc2.getgRpcService()).build().start();
    Server server3 = InProcessServerBuilder.forName(ownerName3).directExecutor()
            .addService(rpc3.getgRpcService()).build().start();
    Server server4 = InProcessServerBuilder.forName(ownerName4).directExecutor()
            .addService(rpc4.getgRpcService()).build().start();
    grpcCleanup.register(server0);
    grpcCleanup.register(server1);
    grpcCleanup.register(server2);
    grpcCleanup.register(server3);
    grpcCleanup.register(server4);
  }

  void testMultiply(long taskId, List<SecretMultiply> executors, List<Integer> integers, long ans)
      throws InterruptedException, ExecutionException {
    List<Integer> parties = executors.stream().map(e -> e.getOwnId()).collect(Collectors.toList());
    List<Future<Object>> futures = new ArrayList<>();
    for (int i = 0; i < executors.size(); ++i) {
      final SecretMultiply s = executors.get(i);
      final int int1 = integers.get(2 * i);
      final int int2 = integers.get(2 * i + 1);
      futures.add(threadpool.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          return s.run(taskId, parties, (long) int1, (long) int2);
        }
      }));
    }
    long res = (long) futures.get(0).get();
    assertEquals(ans, res);
  }

  @Test
  public void testSecretMultiply() throws InterruptedException, ExecutionException {
    Random random = new Random();
    OpenHuFuRpc rpc0 = (OpenHuFuRpc) manager.getRpc(0);
    OpenHuFuRpc rpc1 = (OpenHuFuRpc) manager.getRpc(1);
    OpenHuFuRpc rpc2 = (OpenHuFuRpc) manager.getRpc(2);
    OpenHuFuRpc rpc3 = (OpenHuFuRpc) manager.getRpc(3);
    OpenHuFuRpc rpc4 = (OpenHuFuRpc) manager.getRpc(4);
    List<OpenHuFuRpc> rpcs = ImmutableList.of(rpc0, rpc1, rpc2, rpc3, rpc4);
    List<SecretMultiply> executors =
        rpcs.stream().map(rpc -> new SecretMultiply(rpc)).collect(Collectors.toList());
    List<Integer> integers = new ArrayList<>();
    long u = 0;
    long v = 0;
    for (int i = 0; i < 5; i++) {
      int int1 = random.nextInt(128);
      int int2 = random.nextInt(128);
      integers.add(int1);
      integers.add(int2);
      u += int1;
      v += int2;
    }
    testMultiply(0, executors, integers, u * v);
  }
}
