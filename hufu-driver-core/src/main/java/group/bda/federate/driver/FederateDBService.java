package group.bda.federate.driver;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.Empty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.math3.distribution.LaplaceDistribution;

import group.bda.federate.client.FederateDBClient;
import group.bda.federate.config.FedSpatialConfig;
import group.bda.federate.data.DataSet;
import group.bda.federate.data.Header;
import group.bda.federate.data.RandomDataSet;
import group.bda.federate.data.Row;
import group.bda.federate.data.Stream2BatchDataSet;
import group.bda.federate.data.StreamDataSet;
import group.bda.federate.data.StreamObserverDataSet;
import group.bda.federate.rpc.FederateCommon.DataSetProto;
import group.bda.federate.rpc.FederateCommon.FederateDataSet;
import group.bda.federate.rpc.FederateCommon.FederateTableInfo;
import group.bda.federate.rpc.FederateCommon.HeaderProto;
import group.bda.federate.rpc.FederateCommon.Point;
import group.bda.federate.rpc.FederateGrpc;
import group.bda.federate.rpc.FederateService;
import group.bda.federate.rpc.FederateService.AddClientRequest;
import group.bda.federate.rpc.FederateService.CacheID;
import group.bda.federate.rpc.FederateService.Code;
import group.bda.federate.rpc.FederateService.GeneralResponse;
import group.bda.federate.rpc.FederateService.GetMulValueRequest;
import group.bda.federate.rpc.FederateService.GetRowsResponse;
import group.bda.federate.rpc.FederateService.GetTableHeaderRequest;
import group.bda.federate.rpc.FederateService.GridSetRequest;
import group.bda.federate.rpc.FederateService.KnnRadiusQueryResponse;
import group.bda.federate.rpc.FederateService.Order;
import group.bda.federate.rpc.FederateService.PrivacyCompareRequest;
import group.bda.federate.rpc.FederateService.PrivacyCompareResponse;
import group.bda.federate.rpc.FederateService.PrivacyQuery;
import group.bda.federate.rpc.FederateService.QValue;
import group.bda.federate.rpc.FederateService.Query;
import group.bda.federate.rpc.FederateService.SetUnionRequest;
import group.bda.federate.rpc.FederateService.Status;
import group.bda.federate.rpc.FederateService.TraversalTable;
import group.bda.federate.rpc.FederateService.Value;
import group.bda.federate.security.dp.Laplace;
import group.bda.federate.security.secretsharing.ShamirSharing;
import group.bda.federate.security.secretsharing.utils.MulCache;
import group.bda.federate.security.union.RandomSharesSetUnion;
import group.bda.federate.driver.ir.IRChecker;
import group.bda.federate.driver.table.ServerTableInfo;
import group.bda.federate.driver.utils.AggCache;
import group.bda.federate.driver.utils.ConcurrentBuffer;
import group.bda.federate.driver.utils.DistanceDataSet;
import io.grpc.stub.StreamObserver;

public abstract class FederateDBService extends FederateGrpc.FederateImplBase {
  private static final Logger LOG = LogManager.getLogger(FederateDBService.class);
  protected final Map<String, FederateDBClient> federateClientMap;
  protected final Map<String, ServerTableInfo> tableInfoMap;
  protected final Lock clientLock;
  protected ConcurrentBuffer buffer;
  protected Random random;
  private final int THREAD_POOL_SIZE;
  private final ExecutorService executorService;
  private final Laplace lp;

  FederateDBService(int threadNum) {
    federateClientMap = new TreeMap<>();
    tableInfoMap = new TreeMap<>();
    clientLock = new ReentrantLock();
    buffer = new ConcurrentBuffer();
    random = new Random();
    THREAD_POOL_SIZE = threadNum;
    this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    this.lp = new Laplace(FedSpatialConfig.EPS_DP, FedSpatialConfig.SD_DP);
  }

  FederateDBService() {
    this(FedSpatialConfig.SERVER_THREAD_NUM);
  }

  /*
   * functions below need to be implemented in subclass
   */

  public DistanceDataSet calKnn(Query query)
          throws UnsupportedOperationException, SQLException {
    throw new UnsupportedOperationException("not support knn with column filter");
  }

  public void fedSpatialQueryInternal(Query request, StreamDataSet streamDataSet) throws SQLException {
    throw new UnsupportedOperationException("not support fedSpatialQuery");
  }

  /*
   * federateDBService core functions
   */

  public boolean isTableExist(String tableName) {
    return tableInfoMap.containsKey(tableName);
  }

  @Override
  public void fedSpatialQuery(Query request, StreamObserver<DataSetProto> observer) {
    final Header header = Header.fromProto(request.getHeader());
    if (!(new IRChecker(request.getProjectExpList(), getTableHeader(request.getTableName()), header).check())) {
      LOG.warn("Privacy level check failed!");
      observer.onCompleted();
      return;
    }
    StreamDataSet streamDataSet = new StreamObserverDataSet(observer, header);
    try {
      fedSpatialQueryInternal(request, streamDataSet);
    } catch (SQLException e) {
      LOG.error("error when query table [{}]", request.getTableName());
      e.printStackTrace();
    }
    streamDataSet.close();
  }

  private String getThirdEndpoint(List<String> endpoints, int i, int j, int n) {
    return endpoints.get((((i + 1) % n) == j) ? (i + 2) % n : (i + 1) % n);
  }

  @Override
  public void privacyCompare(PrivacyCompareRequest request, StreamObserver<PrivacyCompareResponse> observer) {
    final List<String> endpoints = request.getEndpointsList();
    final String uuid =  request.getUuid();
    final int n = endpoints.size();
    int idx = request.getIdx();
    List<Integer> shares = new ArrayList<>();
    // step1: get local count, generate mulcache
    if (!(buffer.get(request.getCacheid()) instanceof DistanceDataSet)) {
      LOG.error("please use knn Radius first");
      observer.onCompleted();
    }
    DistanceDataSet distanceDataSet = buffer.getDistanceDataSet(request.getCacheid());
    int u = distanceDataSet.getRangeCount(request.getRadius()) - request.getX();
    if (n <= 2) {
      shares.add(u);
      observer.onNext(PrivacyCompareResponse.newBuilder().addAllShares(shares).build());
      observer.onCompleted();
      return;
    }
    MulCache mc = new MulCache(n);
    buffer.set(uuid, mc);
    int v = mc.getRan(idx, true);
    LOG.debug("in privacy knn local radius: {} , count: {}, X: {}, random: {}", request.getRadius(), u + request.getX(), request.getX(), v);
    shares.add(u * v);
    shares.add(mc.ranSum(idx));
    // step2: get a_k from third party and calculate v_i + a_k
    List<Callable<Integer>> task1 = new ArrayList<>();
    final int[][] vals = new int[n][2];
    for (int i = 0; i < n; ++i) {
      if (i == idx) continue;
      final int j = i;
      task1.add(() -> {
        FederateDBClient client = federateClientMap.get(getThirdEndpoint(endpoints, idx, j, n));
        int result = client.getMulValue(GetMulValueRequest.newBuilder().setIdx(idx).setIsFirst(true).setIsRan(true).setUuid(uuid).build());
        vals[j][1] = u + result;
        LOG.debug("get Random {} from {} in case {} {}", result, client.getEndpoint(), idx, j);
        return result;
      });
      task1.add(() -> {
        FederateDBClient client = federateClientMap.get(getThirdEndpoint(endpoints, j, idx, n));
        int result =  client.getMulValue(GetMulValueRequest.newBuilder().setIdx(j).setIsFirst(false).setIsRan(true).setUuid(uuid).build());
        vals[j][0] = v + result;
        LOG.debug("get Random {} from {} in case {} {}", result, client.getEndpoint(), j, idx);
        return result;
      });
    }
    try {
      List<Future<Integer>> alphaList = executorService.invokeAll(task1);
      for (Future<Integer> falpha : alphaList) {
        falpha.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    mc.setVal(vals);
    // step3: get x_j + a_k from j and cal shares
    List<Callable<Integer>> task2 = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      if (i == idx) continue;
      final int j = i;
      task2.add(() -> {
        FederateDBClient client = federateClientMap.get(endpoints.get(j));
        int result = client.getMulValue(GetMulValueRequest.newBuilder().setIdx(idx).setIsFirst(true).setIsRan(false).setUuid(uuid).build());
        LOG.debug("get val {} share {} from {} in case {} {}", result, -vals[j][1] * result + u * result, client.getEndpoint(), j, idx);
        return -vals[j][1] * result + u * result;
      });
      task2.add(() -> {
        FederateDBClient client = federateClientMap.get(endpoints.get(j));
        int result =  client.getMulValue(GetMulValueRequest.newBuilder().setIdx(idx).setIsFirst(false).setIsRan(false).setUuid(uuid).build());
        LOG.debug("get val {} share {} from {} in case {} {}", result, result * v, client.getEndpoint(), j, idx);
        return result * v;
      });
    }
    try {
      int sum = 0;
      List<Future<Integer>> sList = executorService.invokeAll(task2);
      for (Future<Integer> s : sList) {
        sum += s.get();
      }
      shares.add(sum);
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    observer.onNext(PrivacyCompareResponse.newBuilder().addAllShares(shares).build());
    observer.onCompleted();
  }

  @Override
  public void getMulValue(GetMulValueRequest request, StreamObserver<Value> observer) {
    String uuid = request.getUuid();
    MulCache mc = buffer.getMulCache(uuid);
    if (request.getIsRan()) {
      observer.onNext(Value.newBuilder().setVal(mc.getRan(request.getIdx(), request.getIsFirst())).build());
    } else {
      observer.onNext(Value.newBuilder().setVal(mc.getVal(request.getIdx(), request.getIsFirst())).build());
    }
    observer.onCompleted();
  }

  @Override
  public void fedSpatialPrivacyQuery(final PrivacyQuery request, StreamObserver<DataSetProto> observer) {
    Query query = request.getQuery();
    final Header header = Header.fromProto(query.getHeader());
    if (!(new IRChecker(query.getProjectExpList(), getTableHeader(query.getTableName()), header).check())) {
      LOG.warn("Privacy level check failed!");
      observer.onCompleted();
      return ;
    }
    DataSet dataSet = DataSet.newDataSet(header); 
    Stream2BatchDataSet s2bDataSet = new Stream2BatchDataSet(dataSet);
    // Phase 1:
    try {
      if (request.hasKnnCacheId()) {
        DistanceDataSet distanceDataSet = buffer.getDistanceDataSet(request.getKnnCacheId());
        distanceDataSet.getRangeView(request.getRadius(), s2bDataSet);
      } else {
        fedSpatialQueryInternal(query, s2bDataSet);
      }
    } catch (SQLException e) {
      LOG.error("error when query table [{}]", query.getTableName());
      e.printStackTrace();
    }
    if (query.hasAggUuid()) {
      observer.onNext(DataSet.newDataSet(header).toProto());
      observer.onCompleted();
      return;
    }
    FederateService.SetUnionRequest setUnionRequest = request.getSetUnion();
    // if (setUnionRequest.getAddOrder(0).getEndpointsCount() < 3) {
    //   StreamDataSet streamDataSet = new StreamObserverDataSet(observer, Header.fromProto(request.getQuery().getHeader()));
    //   streamDataSet.addDataSet(s2bDataSet.getDataSet());
    //   streamDataSet.close();
    //   return;
    // }
    int divide = setUnionRequest.getAddOrderCount();
    final List<List<Row>> dividedRows = s2bDataSet.getDivided(divide);
    List<Callable<DataSet>> unionTasks = new ArrayList<>();
    // add loop
    boolean isLeader = false;
    for (int i = 0; i < divide; i++) {
      final Order addOrder = setUnionRequest.getAddOrder(i);
      final Order delOrder = setUnionRequest.getDelOrder(i);
      final int idx= i;
      if (addOrder.getIndex() == 0) {
        // leader
        isLeader = true;
        unionTasks.add(() -> {
          LOG.debug("I'm leader in order add:{} del:{}", addOrder.getUuid(), delOrder.getUuid());
          RandomDataSet localRandomSet = RandomSharesSetUnion.generateRandomSet(header, dividedRows.get(idx));
          // leader doesn't need to get predecessor's data set local directly
          DataSet dataset = localRandomSet.getRandomSet();
          LOG.debug("generate random dataset with size {}", dataSet.rowCount());
          buffer.set(addOrder.getUuid(), dataset);
          // leader get the final result of add round and remove random data
          dataset = RandomSharesSetUnion.removeRandomSet(getPredecessorDataSet(request, idx, true), localRandomSet);
          buffer.set(delOrder.getUuid(), dataset);
          return getPredecessorDataSet(request, idx, false);
        });
      } else {
        // follower
        unionTasks.add(() -> {
          LOG.debug("I'm follower in order add:{} del:{}", addOrder.getUuid(), delOrder.getUuid());
          RandomDataSet localRandomSet = RandomSharesSetUnion.generateRandomSet(header, dividedRows.get(idx));
          // follwer add random data
          DataSet dataset = localRandomSet.getRandomSet();
          dataset.mergeDataSetUnsafe(getPredecessorDataSet(request, idx, true));
          LOG.debug("mix dataset with size {}", dataSet.rowCount());
          buffer.set(addOrder.getUuid(), dataset);
          // follower delete random data
          dataset = RandomSharesSetUnion.removeRandomSet(getPredecessorDataSet(request, idx, false), localRandomSet);
          buffer.set(delOrder.getUuid(), dataset);
          return DataSet.newDataSet(header);
        });
      }
    }
    DataSet result = DataSet.newDataSet(header);
    try {
      List<Future<DataSet>> results = executorService.invokeAll(unionTasks);
      for (Future<DataSet> res : results) {
        DataSet s = res.get();
        if (s == null) {
          LOG.error("error when secure set union");
        }
        synchronized (result) {
          result.mergeDataSetUnsafe(s);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (isLeader) {
      StreamDataSet streamDataSet = new StreamObserverDataSet(observer, Header.fromProto(query.getHeader()));
      LOG.info("result size {}", result.rowCount());
      streamDataSet.addDataSet(result);
      streamDataSet.close();
    } else {
      observer.onNext(DataSet.newDataSet(header).toProto());
      observer.onCompleted();
    }
  }

  private DataSet getPredecessorDataSet(PrivacyQuery query, int loop, boolean isAdd) {
    FederateService.SetUnionRequest setUnionRequest = query.getSetUnion();
    Header header = Header.fromProto(query.getQuery().getHeader());
    FederateService.Order order;
    if (loop >= setUnionRequest.getAddOrderCount()) {
      order = null;
    } else if (isAdd) {
      order = setUnionRequest.getAddOrder(loop);
    } else {
      order = setUnionRequest.getDelOrder(loop);
    }
    if (order == null) {
      LOG.error("Fail to find predecessor");
      return null;
    }
    String preEndpoints;
    if (order.getIndex() != 0) {
      preEndpoints = order.getEndpoints((order.getIndex() - 1) % order.getEndpointsCount());
    } else {
      preEndpoints = order.getEndpoints(order.getEndpointsCount() - 1);
    }
    String uuid = order.getUuid();
    DataSet dataSet = DataSet.newDataSet(header, uuid);
    FederateDBClient client = federateClientMap.get(preEndpoints);
    LOG.debug("get dataset from {} with uuid {}", preEndpoints, uuid);
    Iterator<DataSetProto> dIterator = client.getRandomDataSet(uuid);
    while (dIterator.hasNext()) {
      DataSet tmp = DataSet.fromProto(dIterator.next());
      dataSet.mergeDataSetUnsafe(tmp);
    }
    return dataSet;
  }

  @Override
  public void addClient(final AddClientRequest request, final StreamObserver<GeneralResponse> observer) {
    observer.onNext(addClient(request.getEndpoint()));
    observer.onCompleted();
  }

  @Override
  public void getTableHeader(GetTableHeaderRequest request, StreamObserver<HeaderProto> observer) {
    HeaderProto headerProtocol = getTableHeader(request.getTableName()).toProto();
    observer.onNext(headerProtocol);
    observer.onCompleted();
  }

  @Override
  public void getRandomDataSet(FederateService.CacheID request, StreamObserver<DataSetProto> responseObserver) {
    String uuid = request.getUuid();
    DataSet dataSet = null;
    dataSet = buffer.getDataSet(uuid);
    if (dataSet == null) {
      LOG.error("fail to get random dataset with uuid {}", uuid);
      responseObserver.onCompleted();
    } else {
      LOG.info("in uuid {} get mixDataSet size is {} ", uuid, dataSet.rowCount());
      buffer.remove(uuid);
      StreamDataSet streamDataSet = new StreamObserverDataSet(responseObserver, dataSet);
      streamDataSet.close();
    }
  }

  public Header getTableHeader(String tableName) {
    ServerTableInfo info = tableInfoMap.get(tableName);
    if (info == null) {
      return Header.newBuilder(0).build();
    } else {
      return info.getHeader();
    }
  }

  @Override
  public void clearCache(CacheID request, StreamObserver<Empty> observer) {
    String uuid = request.getUuid();
    buffer.remove(uuid);
    observer.onNext(Empty.newBuilder().build());
    observer.onCompleted();
  }

  private GeneralResponse addClient(String endpoint) {
    synchronized (clientLock) {
      if (!federateClientMap.containsKey(endpoint)) {
        FederateDBClient client = new FederateDBClient(endpoint);
        federateClientMap.put(endpoint, client);
        ShamirSharing.addCoeList();
      }
    }
    LOG.debug("add federateClientMap [{}]", endpoint);
    return GeneralResponse.newBuilder().setStatus(Status.newBuilder().setCode(Code.kOk).setMsg("ok").build()).build();
  }

  @Override
  public void knnRadiusQuery(FederateService.KnnRadiusQueryRequest request, StreamObserver<KnnRadiusQueryResponse> responseObserver) {
    responseObserver.onNext(knnRadiusQuery(request.getQuery(), request.getUuid()));
    responseObserver.onCompleted();
  }

  private double bufferCount(FederateService.PrivacyCountRequest request) {
    Object o = buffer.get(request.getCacheUuid());
    if (o instanceof DistanceDataSet) {
      DistanceDataSet distanceDataSet = (DistanceDataSet) o;
      int count = distanceDataSet.getRangeCount(request.getRadius());
      LOG.debug("in privacy knn local radius: {} , count: {} ", request.getRadius(), count);
      return count;
    } else {
      AggCache aggCache = (AggCache) o;
      double res = aggCache.getColumn(request.getColumnId());
      LOG.info("in privacy aggregate the result is {}", res);
      return res;
    }
  }

  // private PrivacyCountResponse privacyCount(FederateService.PrivacyCountRequest request) {
  //   double value = bufferCount(request);
  //   String uuid = request.getUuid();
  //   ShamirCache cache = new ShamirCache();
  //   List<Integer> x = request.getXList();
  //   List<String> endpoints = request.getEndpoints();
  //   buffer.set(uuid, cache);
  //   for (int i = 0; i < x.size(); i++) {
  //     double qxi = calQValue(x.get(i), value, x.size() - 1);
  //     String endpoint = endpoints.get(i);
  //     if (federateClientMap.containsKey(endpoint)) {
  //       federateClientMap.get(endpoint).sendQValue(qxi, uuid);
  //     } else {
  //       cache.setQValue(qxi);
  //     }
  //   }
  // }

  @Override
  public void privacyCount(final FederateService.PrivacyCountRequest request,
                           final StreamObserver<FederateService.PrivacyCountResponse> observer) {
    double value = bufferCount(request);
    observer.onNext(ShamirSharing.privacyCount(value, request.getXList(), request.getEndpointsList(),
            request.getUuid(), federateClientMap));
    observer.onCompleted();
  }

  public double dpCount(FederateService.PrivacyCountRequest request) {
    DistanceDataSet distanceDataSet = buffer.getDistanceDataSet(request.getCacheUuid());
    double count = (double) distanceDataSet.getRangeCount(request.getRadius()) + lp.sample();
    LOG.debug("in knn local radius: {} , dp count: {} ", request.getRadius(), count);
    return count;
  }

  @Override
  public void dPRangeCount(final FederateService.PrivacyCountRequest request,
      final StreamObserver<FederateService.DPRangeCountResponse> observer) {
    double value = dpCount(request);
    observer.onNext(FederateService.DPRangeCountResponse.newBuilder().setSd(lp.getSD()).setResult(value).build());
    observer.onCompleted();
  }
  @Override
  public void sendQValue(final QValue request, final StreamObserver<GeneralResponse> observer) {
    observer.onNext(ShamirSharing.revQValue(request.getUuid(), request.getQ()));
    observer.onCompleted();
  }

  @Override
  public void getSum(final CacheID request, final StreamObserver<FederateService.PrivacyCountResponse> observer) {
    observer.onNext(ShamirSharing.getSum(request.getUuid()));
    observer.onCompleted();
  }

  public KnnRadiusQueryResponse knnRadiusQuery(Query query, String uuid) throws RuntimeException {
    final KnnRadiusQueryResponse.Builder responseBuilder = KnnRadiusQueryResponse.newBuilder();
    DistanceDataSet results;
    
    if (uuid.length() != 0 && buffer.contains(uuid)) {
      results = buffer.getDistanceDataSet(uuid);
    } else {
      try {
        results = calKnn(query);
        buffer.set(uuid, results);
      } catch (SQLException e) {
        LOG.error("error when calculate local knn");
        e.printStackTrace();
        responseBuilder.setRadius(Double.MAX_VALUE);
        return responseBuilder.build();
      }
    }
    int k = query.getFetch();
    double distance = FedSpatialConfig.KNN_RADIUS;
    double delt = distance;
    if (results.size() >= k) {
      distance = results.getDistance(k - 1);
      if (k - 2 >= 0) {
        delt = distance - results.getDistance(k - 2);
      } else {
        delt = distance;
      }
      LOG.debug("{} nn circle radius is {}", k, distance);
    } else if (results.size() > 0) {
      distance = results.getDistance(results.size() - 1);
      if (results.size() > 1) {
        delt = distance - results.getDistance(results.size() - 2);
      } else {
        delt = distance;
      }
      LOG.debug("{} nn circle radius is {}", results.size() - 1, distance);
    }
    LOG.info("DP delt = {}", delt);
    distance += Math.abs(new LaplaceDistribution(0, delt / FedSpatialConfig.EPS_DP).sample());
    responseBuilder.setRadius(distance);
    return responseBuilder.build();
  }
}