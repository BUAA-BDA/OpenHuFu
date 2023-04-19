package com.hufudb.openhufu.mpc.secretUnion;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.ArrayDataSet;
import com.hufudb.openhufu.data.storage.ArrayRow;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.DataSetIterator;
import com.hufudb.openhufu.mpc.union.SecretUnion;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class SecretUnionTest {
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

  void testUnion(long taskId, List<SecretUnion> executors, List<DataSet> dataSets, List<List<Object>> ans)
      throws InterruptedException, ExecutionException {
    List<Integer> parties = executors.stream().map(e -> e.getOwnId()).collect(Collectors.toList());
    List<Future<Object>> futures = new ArrayList<>();
    for (int i = 0; i < executors.size(); ++i) {
      final SecretUnion s = executors.get(i);
      final DataSet dataSet = dataSets.get(i);
      futures.add(threadpool.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          return s.run(taskId, parties, dataSet);
        }
      }));
    }
    List<List<Object>> resList = new ArrayList<>();
    DataSet res = (DataSet) futures.get(0).get();
    DataSetIterator it = res.getIterator();
    while (it.next()) {
      resList.add(ImmutableList.of(it.get(0), it.get(1), it.get(2)));
    }
    sort(resList);
    assertEquals(resList.size(), ans.size());
    for (int i = 0; i < resList.size(); i++) {
      assertEquals(resList.get(i).get(0), ans.get(i).get(0));
      assertEquals(resList.get(i).get(1), ans.get(i).get(1));
      assertEquals(resList.get(i).get(2), ans.get(i).get(2));
    }
  }

  private void sort(List<List<Object>> lists) {
    lists.sort(new Comparator<List<Object>>() {
      @Override
      public int compare(List<Object> o1, List<Object> o2) {
        Double double1 = (Double) o1.get(0);
        Double double2 = (Double) o2.get(0);
        Integer int1 = (Integer) o1.get(1);
        Integer int2 = (Integer) o2.get(1);
        Point point1 = (Point) o1.get(2);
        Point point2 = (Point) o2.get(2);
        if (double1 < double2) {
          return -1;
        }
        else if (double1 > double2) {
          return 1;
        }
        if (!Objects.equals(int1, int2)) {
          return int1 - int2;
        }
        return point1.compareTo(point2);
      }
    });
  }

  @Ignore
  @Test
  public void testSecretUnion() throws InterruptedException, ExecutionException {
    Random random = new Random();
    OpenHuFuRpc rpc0 = (OpenHuFuRpc) manager.getRpc(0);
    OpenHuFuRpc rpc1 = (OpenHuFuRpc) manager.getRpc(1);
    OpenHuFuRpc rpc2 = (OpenHuFuRpc) manager.getRpc(2);
    OpenHuFuRpc rpc3 = (OpenHuFuRpc) manager.getRpc(3);
    OpenHuFuRpc rpc4 = (OpenHuFuRpc) manager.getRpc(4);
    List<OpenHuFuRpc> rpcs = ImmutableList.of(rpc0, rpc1, rpc2, rpc3, rpc4);
    List<SecretUnion> executors =
        rpcs.stream().map(rpc -> new SecretUnion(rpc)).collect(Collectors.toList());
    List<DataSet> dataSets = new ArrayList<>();
    List<List<Object>> ans = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      List<ArrayRow> arrayRows = new ArrayList<>();
      for (int j = 0; j < random.nextInt(20); j++) {
        ArrayRow.Builder builder = ArrayRow.newBuilder(3);
        Double randDouble = random.nextDouble();
        Integer randInt = random.nextInt();
        Point randPoint = geoFactory.createPoint(new Coordinate(random.nextDouble(), random.nextDouble()));
        builder.set(0, randDouble);
        builder.set(1, randInt);
        builder.set(2, randPoint);
        ans.add(ImmutableList.of(randDouble, randInt, randPoint));
        arrayRows.add(builder.build());
      }
      dataSets.add(new ArrayDataSet(Schema.newBuilder()
              .add("DOUBLE", ColumnType.DOUBLE)
              .add("INT", ColumnType.INT)
              .add("POINT", ColumnType.GEOMETRY)
              .build(), arrayRows));
    }
    sort(ans);
    testUnion(0, executors, dataSets, ans);
  }
}
