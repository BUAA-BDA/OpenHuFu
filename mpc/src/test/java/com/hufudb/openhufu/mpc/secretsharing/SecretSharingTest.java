package com.hufudb.openhufu.mpc.secretsharing;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
import com.hufudb.openhufu.rpc.Party;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuOwnerInfo;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpc;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuRpcManager;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Ignore;

@RunWith(JUnit4.class)
public class SecretSharingTest {

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

  void testCaseLong(long taskId, List<SecretSharing> executors, List<Long> values)
      throws InterruptedException, ExecutionException {
    List<Integer> parties = executors.stream().map(e -> e.getOwnId()).collect(Collectors.toList());
    List<Future<Object>> futures = new ArrayList<>();
    for (int i = 0; i < executors.size(); ++i) {
      final SecretSharing s = executors.get(i);
      final long v = values.get(i);
      futures.add(threadpool.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          return s.run(taskId, parties, ColumnType.LONG, v, OperatorType.PLUS);
        }
      }));
    }
    Object res = futures.get(0).get();
    long expect = values.stream().reduce(0L, (t, v) -> t + v);
    assertEquals(expect, (long) res);
  }

  void testCaseDouble(long taskId, List<SecretSharing> executors, List<Double> values) throws InterruptedException, ExecutionException {
    List<Integer> parties = executors.stream().map(e -> e.getOwnId()).collect(Collectors.toList());
    List<Future<Object>> futures = new ArrayList<>();
    for (int i = 0; i < executors.size(); ++i) {
      final SecretSharing s = executors.get(i);
      final double v = values.get(i);
      futures.add(threadpool.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          return s.run(taskId, parties, ColumnType.DOUBLE, v, OperatorType.PLUS);
        }
      }));
    }
    Object res = futures.get(0).get();
    double expect = values.stream().reduce(0.0, (t, v) -> t + v);
    assertEquals(expect, (double) res, 0.00001);
  }

  @Ignore
  @Test
  public void testSecretSharing() throws InterruptedException, ExecutionException {
    Random random = new Random();
    OpenHuFuRpc rpc0 = (OpenHuFuRpc) manager.getRpc(0);
    OpenHuFuRpc rpc1 = (OpenHuFuRpc) manager.getRpc(1);
    OpenHuFuRpc rpc2 = (OpenHuFuRpc) manager.getRpc(2);
    OpenHuFuRpc rpc3 = (OpenHuFuRpc) manager.getRpc(3);
    OpenHuFuRpc rpc4 = (OpenHuFuRpc) manager.getRpc(4);
    List<OpenHuFuRpc> rpcs = ImmutableList.of(rpc0, rpc1, rpc2, rpc3, rpc4);
    List<SecretSharing> executors =
        rpcs.stream().map(rpc -> new SecretSharing(rpc)).collect(Collectors.toList());
    testCaseLong(0L, executors, ImmutableList.of(1L, 2L, 3L, 4L, 5L));
    testCaseLong(1L, executors.subList(0, 3), ImmutableList.of(123L, 321L, 999L));
    testCaseDouble(2L, executors, ImmutableList.of(3.14, 54.12, 99.6, 37.9, 2.78));
    final int ROUND = 10;
    for (int i = 0; i < ROUND; ++i) {
      int partyNum = random.nextInt(4) + 2;
      List<Long> lv = new ArrayList<>();
      List<Double> dv = new ArrayList<>();
      for (int j = 0; j < partyNum; ++j) {
        lv.add(random.nextLong());
        dv.add(random.nextDouble());
      }
      testCaseLong(i * 2 + 3, executors.subList(0, partyNum), lv);
      testCaseDouble(i * 2 + 4, executors.subList(0, partyNum), dv);
    }
  }
}
