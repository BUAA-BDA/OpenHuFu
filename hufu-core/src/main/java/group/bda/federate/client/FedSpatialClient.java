package group.bda.federate.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import group.bda.federate.data.DataSet;
import group.bda.federate.rpc.FederateService;
import group.bda.federate.security.secretsharing.ShamirSharing;
import group.bda.federate.sql.functions.AggregateFunc;
import group.bda.federate.sql.functions.AggregateFuncImpl;
import group.bda.federate.sql.type.FederateFieldType;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.config.FedSpatialConfig;
import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.data.Row;
import group.bda.federate.rpc.FederateCommon.Func;
import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.rpc.FederateCommon.DataSetProto;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.rpc.FederateService.DPRangeCountResponse;
import group.bda.federate.rpc.FederateService.PrivacyCompareRequest;
import group.bda.federate.rpc.FederateService.PrivacyQuery;
import group.bda.federate.rpc.FederateService.Query;
import group.bda.federate.sql.enumerator.JoinEnumerator;
import group.bda.federate.sql.enumerator.RowEnumerator;
import group.bda.federate.sql.enumerator.StreamingIterator;
import group.bda.federate.sql.expression.FedSpatialExpression;
import group.bda.federate.sql.functions.AggregateType;
import group.bda.federate.sql.join.FedSpatialDistanceJoinInfo;
import group.bda.federate.sql.join.FedSpatialJoinInfo;
import group.bda.federate.sql.join.FedSpatialJoinInfo.JoinType;
import group.bda.federate.sql.join.FedSpatialKnnJoinInfo;
import group.bda.federate.sql.operator.FedSpatialRel;
import group.bda.federate.sql.table.FederateTableInfo;
import group.bda.federate.sql.type.Point;

public class FedSpatialClient {
  private static final Logger LOG = LogManager.getLogger(FedSpatialClient.class);

  private final Map<String, FederateDBClient> dbClientMap;
  private final Map<String, FederateTableInfo> tableMap;
  private final ExecutorService executorService;
  private final boolean USE_DP = FedSpatialConfig.USE_DP;

  public FedSpatialClient() {
    dbClientMap = new HashMap<>();
    tableMap = new HashMap<>();
    this.executorService = Executors.newFixedThreadPool(FedSpatialConfig.CLIENT_THREAD_NUM);
  }

  public Map<String, FederateDBClient> getDBClientMap() {
    return dbClientMap;
  }

  public Map<String, FederateTableInfo> getTableMap() {
    return tableMap;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  // for federateDB
  public boolean addFederate(String endpoint) {
    if (hasFederate(endpoint)) {
      return false;
    }
    FederateDBClient newClient = new FederateDBClient(endpoint);
    for (Map.Entry<String, FederateDBClient> entry : dbClientMap.entrySet()) {
      entry.getValue().addClient(endpoint);
      newClient.addClient(entry.getKey());
    }
    dbClientMap.put(endpoint, newClient);
    return true;
  }

  public boolean hasFederate(String endpoint) {
    return dbClientMap.containsKey(endpoint);
  }

  public FederateDBClient getDBClient(String endpoint) {
    return dbClientMap.get(endpoint);
  }

  // for global table
  public void addTable(String tableName, FederateTableInfo table) {
    this.tableMap.put(tableName, table);
  }

  public void dropTable(String tableName) {
    tableMap.remove(tableName);
  }

  public FederateTableInfo getTable(String tableName) {
    return tableMap.get(tableName);
  }

  public boolean hasTable(String tableName) {
    return tableMap.containsKey(tableName);
  }

  // for local table
  public void addLocalTable(String globalTableName, FederateDBClient client, String localTableName) {
    FederateTableInfo table = getTable(globalTableName);
    if (table != null) {
      table.addFed(client, localTableName);
    }
  }

  public Header getHeader(String tableName) {
    FederateTableInfo table = getTable(tableName);
    return table != null ? table.getHeader() : null;
  }

  public Header generateHeader(String tableName, List<String> columns) {
    FederateTableInfo table = getTable(tableName);
    return table != null ? table.generateHeader(columns) : null;
  }

  public Map<FederateDBClient, String> getTableClients(String tableName) {
    FederateTableInfo table = getTable(tableName);
    return table != null ? table.getTableMap() : null;
  }

  public void clearCache(String uuid, Map<FederateDBClient, String> tableClients) {
    for (Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      FederateDBClient client = entry.getKey();
      client.clearCache(uuid);
    }
  }

  private Map<String, FederateService.SetUnionRequest> genSetUnionRequest(Map<FederateDBClient, String> tableClients) {
    List<String> endpoints = tableClients.keySet().stream().map(FederateDBClient::getEndpoint)
        .collect(Collectors.toList());
    Collections.shuffle(endpoints);
    String header = endpoints.remove(0);
    List<FederateService.Order.Builder> orders = new ArrayList<>();
    for (int i = 0; i < FedSpatialConfig.SET_UNION_DIVIDE * 2; i++) {
      FederateService.Order.Builder order = FederateService.Order.newBuilder();
      order.setUuid(UUID.randomUUID().toString());
      order.addEndpoints(header);
      Collections.shuffle(endpoints);
      order.addAllEndpoints(endpoints);
      orders.add(order);
    }
    Map<String, FederateService.SetUnionRequest> res = new HashMap<>();
    for (FederateDBClient federateDBClient : tableClients.keySet()) {
      FederateService.SetUnionRequest.Builder builder = FederateService.SetUnionRequest.newBuilder();
      String endpoint = federateDBClient.getEndpoint();
      for (int i = 0; i < FedSpatialConfig.SET_UNION_DIVIDE; i++) {
        FederateService.Order.Builder add = orders.get(i * 2);
        FederateService.Order.Builder del = orders.get(i * 2 + 1);
        builder.addAddOrder(add.setIndex(add.getEndpointsList().indexOf(endpoint)).build())
            .addDelOrder(del.setIndex(del.getEndpointsList().indexOf(endpoint)).build());
      }
      res.put(federateDBClient.getEndpoint(), builder.build());
    }
    return res;
  }

  // base function for two table join
  public Enumerator<Row> fedSpatialJoin(FedSpatialRel.SingleQuery left, FedSpatialRel.SingleQuery right,
      FedSpatialJoinInfo joinInfo, List<Integer> projects,
      List<Map.Entry<AggregateType, List<Integer>>> aggregateFields, int fetch, List<String> order) {
    // get left table
    String leftName = left.tableName;
    List<Expression> leftProjects = left.getProjectExps();
    Expression leftFilter = left.getFilterExp();

    String rightName = right.tableName;
    final List<Expression> rightProjects = right.getProjectExps();
    final Expression rightFilter = right.getFilterExp();
    // loop join
    Enumerator<Row> leftEnumerator = fedSpatialQuery(leftName, leftProjects,
        leftFilter == null ? ImmutableList.of() : ImmutableList.of(leftFilter));
    Expression rightKey = rightProjects.get(joinInfo.getRightKey()).toBuilder().setLevel(Level.HIDE.ordinal()).build();
    rightProjects.set(joinInfo.getRightKey(), rightKey);
    if (joinInfo.getType().equals(JoinType.KNN)) {
      final FedSpatialKnnJoinInfo knnJoinInfo = (FedSpatialKnnJoinInfo) joinInfo;
      return new JoinEnumerator(leftEnumerator, knnJoinInfo, projects, (row) -> {
        Point p = (Point) row.getObject(knnJoinInfo.leftKey);
        Expression joinFilter = knnJoinInfo.getKNNFilter(p);
        return fedSpatialQuery(rightName, rightProjects,
            rightFilter == null ? ImmutableList.of(joinFilter) : ImmutableList.of(rightFilter, joinFilter));
      });
    } else {
      final FedSpatialDistanceJoinInfo distanceJoinInfo = (FedSpatialDistanceJoinInfo) joinInfo;
      return new JoinEnumerator(leftEnumerator, joinInfo, projects, (row) -> {
        Point p = (Point) row.getObject(distanceJoinInfo.leftKey);
        Expression joinFilter = distanceJoinInfo.getDistanceFilter(p);
        return fedSpatialQuery(rightName, rightProjects,
            rightFilter == null ? ImmutableList.of(joinFilter) : ImmutableList.of(rightFilter, joinFilter));
      });
    }
  }

  public Enumerator<Row> fedSpatialQuery(String tableName, List<Expression> projects, List<Expression> filter) {
    return fedSpatialQuery(tableName, projects, filter, ImmutableList.of(), Integer.MAX_VALUE, ImmutableList.of());
  }

  public Enumerator<Row> fedSpatialQuery(String tableName, List<Expression> projects, List<Expression> filter,
      int fetch, List<String> order) {
    return fedSpatialQuery(tableName, projects, filter, ImmutableList.of(), fetch, order);
  }

  // base function for single table federate query
  public Enumerator<Row> fedSpatialQuery(String tableName, List<Expression> projects, List<Expression> filter,
      List<Map.Entry<AggregateType, List<Integer>>> aggregateFields, int fetch, List<String> order) {
    Map<FederateDBClient, String> tableClients = getTableClients(tableName);
    StreamingIterator<DataSetProto> streamProto;
    // todo: generate header in converter
    Header header;
    String aggUuid = UUID.randomUUID().toString();
    if (!projects.isEmpty()) {
      header = FedSpatialExpression.generateHeaderFromExpression(projects, filter, order, fetch);
    } else {
      header = getHeader(tableName);
    }
    if (header.hasPrivate() || header.size() == 0) {
      LOG.warn("The query does not meet the privacy conditions");
      return RowEnumerator.emptyEnumerator();
    } else if (header.isPrivacyKnn()) {
      streamProto = privacyKnn(header, projects, filter, tableClients, fetch, order);
    } else if (header.hasPrivacy()) {
      streamProto = fedSpatialPrivacyQuery(header, projects, filter, tableClients, fetch, order, aggUuid);
    } else {
      streamProto = fedSpatialPublicQuery(header, projects, filter, tableClients, fetch, order, aggUuid);
    }
    if (!aggregateFields.isEmpty() || !order.isEmpty()) {
      DataSet localSet = DataSet.newDataSet(header);
      while (streamProto.hasNext()) {
        localSet.mergeDataSetUnsafe(DataSet.fromProto(streamProto.next()));
      }
      DataSet dataSet = null;
      if (!aggregateFields.isEmpty()) {
        dataSet = calculateAgg(aggUuid, localSet, aggregateFields, header, tableClients);
      } else {
        dataSet = localSet;
      }
      if (!order.isEmpty()) {
        dataSet.sort(order);
      }
      return new RowEnumerator(dataSet, fetch);
    }
    return new RowEnumerator(streamProto, fetch);
  }

  private StreamingIterator<DataSetProto> fedSpatialPublicQuery(Header header, List<Expression> project,
      List<Expression> filter, Map<FederateDBClient, String> tableClients, int fetch, List<String> order,
      String aggUuid) {
    StreamingIterator<DataSetProto> iterator = new StreamingIterator<>(tableClients.size());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      tasks.add(() -> {
        try {
          Query.Builder queryBuilder = Query.newBuilder().setTableName(entry.getValue()).setHeader(header.toProto())
              .addAllProjectExp(project).addAllFilterExp(filter).setFetch(fetch).addAllOrder(order);
          if (header.isPrivacyAgg()) {
            queryBuilder.setAggUuid(aggUuid);
          }
          Iterator<DataSetProto> it = entry.getKey().fedSpatialQuery(queryBuilder.build());
          while (it.hasNext()) {
            iterator.add(it.next());
          }
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        } finally {
          iterator.finish();
        }
      });
    }
    try {
      List<Future<Boolean>> statusList = executorService.invokeAll(tasks);
      for (Future<Boolean> status : statusList) {
        if (!status.get()) {
          LOG.error("error in fedSpatialPublicQuery");
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return iterator;
  }

  private StreamingIterator<DataSetProto> fedSpatialPrivacyQuery(Header header, List<Expression> project,
      List<Expression> filter, Map<FederateDBClient, String> tableClients, int fetch, List<String> order,
      String aggUuid) {
    StreamingIterator<DataSetProto> iterator = new StreamingIterator<>(tableClients.size());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    Map<String, FederateService.SetUnionRequest> setUnionRequestMap = genSetUnionRequest(tableClients);
    for (Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      tasks.add(() -> {
        try {
          String endpoint = entry.getKey().getEndpoint();
          Query.Builder queryBuilder = Query.newBuilder().setTableName(entry.getValue()).setHeader(header.toProto())
              .addAllProjectExp(project).addAllFilterExp(filter).setFetch(fetch).addAllOrder(order);
          if (header.isPrivacyAgg()) {
            queryBuilder.setAggUuid(aggUuid);
          }
          PrivacyQuery privacyQuery = PrivacyQuery.newBuilder().setQuery(queryBuilder.build())
              .setSetUnion(setUnionRequestMap.get(endpoint)).build();
          Iterator<DataSetProto> it = entry.getKey().fedSpatialPrivacyQuery(privacyQuery);
          while (it.hasNext()) {
            iterator.add(it.next());
          }
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        } finally {
          iterator.finish();
        }
      });
    }
    try {
      List<Future<Boolean>> statusList = executorService.invokeAll(tasks);
      for (Future<Boolean> status : statusList) {
        if (!status.get()) {
          LOG.error("error in fedSpatialPrivacyQuery");
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return iterator;
  }

  private double knnRadiusQuery(Map<FederateDBClient, String> tableClients, Query.Builder query, String uuid) {
    List<Callable<Double>> tasks = new ArrayList<>();
    double min = Double.MAX_VALUE;
    for (Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      tasks.add(() -> entry.getKey().knnRadiusQuery(query.clone().setTableName(entry.getValue()).build(), uuid));
    }
    try {
      List<Future<Double>> results = executorService.invokeAll(tasks);
      for (Future<Double> res : results) {
        double radius = res.get();
        if (radius < min) {
          min = radius;
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return min;
  }

  private StreamingIterator<DataSetProto> knnCircleRangeQuery(Query.Builder query,
      Map<FederateDBClient, String> tableClients, String knnCacheId, double radius) {
    StreamingIterator<DataSetProto> iterator = new StreamingIterator<>(tableClients.size());
    List<Callable<Boolean>> tasks = new ArrayList<>();
    Map<String, FederateService.SetUnionRequest> setUnionRequestMap = genSetUnionRequest(tableClients);
    for (Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      tasks.add(() -> {
        try {
          String endpoint = entry.getKey().getEndpoint();
          PrivacyQuery privacyQuery = PrivacyQuery.newBuilder()
              .setQuery(query.clone().setTableName(entry.getValue()).build())
              .setSetUnion(setUnionRequestMap.get(endpoint)).setKnnCacheId(knnCacheId).setRadius(radius).build();
          Iterator<DataSetProto> it = entry.getKey().fedSpatialPrivacyQuery(privacyQuery);
          while (true) {
            assert it != null;
            if (!it.hasNext())
              break;
            iterator.add(it.next());
          }
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        } finally {
          iterator.finish();
        }
      });
    }
    try {
      List<Future<Boolean>> statusList = executorService.invokeAll(tasks);
      for (Future<Boolean> status : statusList) {
        if (!status.get()) {
          LOG.error("error in fedSpatialPrivacyQuery");
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return iterator;
  }

  private Pair<Double, Double> dPRangeCount(FederateService.PrivacyCountRequest request,
      Map<FederateDBClient, String> tableClients) {
    double result = 0;
    double sd = 0;
    List<Callable<Pair<Double, Double>>> tasks = new ArrayList<>();
    for (Map.Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      tasks.add(() -> {
        try {
          DPRangeCountResponse response = entry.getKey().getDPRangeCountResult(request);
          return Pair.of(response.getResult(), response.getSd());
        } catch (Exception e) {
          e.printStackTrace();
          return Pair.of(0.0, 0.0);
        }
      });
    }
    try {
      List<Future<Pair<Double, Double>>> pairs = executorService.invokeAll(tasks);
      for (Future<Pair<Double, Double>> pair : pairs) {
        Pair<Double, Double> p = pair.get();
        result += p.getKey();
        sd += p.getValue();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return Pair.of(result, sd);
  }

  private int privacyCompare(PrivacyCompareRequest.Builder request, Map<FederateDBClient, String> tableClients, int k) {
    List<Callable<Integer>> tasks = new ArrayList<>();
    String uuid = UUID.randomUUID().toString();
    final List<Pair<FederateDBClient, String>> clients = new ArrayList<>();
    for (Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      clients.add(Pair.of(entry.getKey(), entry.getValue()));
    }
    Collections.shuffle(clients);
    final List<String> endpoints = new ArrayList<>();
    for (Pair<FederateDBClient, String> pair : clients) {
      endpoints.add(pair.getKey().getEndpoint());
    }
    int sk = k / endpoints.size();
    int rk = k - sk * (endpoints.size() - 1);
    request.addAllEndpoints(endpoints).setUuid(uuid);
    for (int i = 0; i < endpoints.size(); ++i) {
      final int j = i;
      final int vk = (j == endpoints.size() - 1) ? rk : sk;
      tasks.add(() -> {
        try {
          return clients.get(j).getKey().privacyCompare(request.clone().setIdx(j).setX(vk).build());
        } catch (Exception e) {
          e.printStackTrace();
          return 0;
        }
      });
    }
    int sum = 0;
    try {
      List<Future<Integer>> sgns = executorService.invokeAll(tasks);
      for (Future<Integer> s : sgns) {
        sum += s.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    if (request.getEndpointsCount() < 2) {
      clearCache(uuid, tableClients);
    }
    return sum;
  }

  private int getK(List<Expression> filters, int fetch) {
    for (Expression e : filters) {
      for (IR ir : e.getIrList()) {
        if (ir.hasFunc() && ir.getFunc() == Func.kKNN) {
          return ir.getInList().get(2).getLiteral().getValue().getI32();
        }
      }
    }
    return fetch;
  }

  private StreamingIterator<DataSetProto> privacyKnn(Header header, List<Expression> project, List<Expression> filter,
      Map<FederateDBClient, String> tableClients, int fetch, List<String> order) {
    String uuid = UUID.randomUUID().toString();
    Query.Builder query = Query.newBuilder().setHeader(header.toProto()).addAllProjectExp(project)
        .addAllFilterExp(filter).setFetch(fetch).addAllOrder(order);
    double right = knnRadiusQuery(tableClients, query.clone(), uuid) * 2;
    if (!USE_DP) {
      right = FedSpatialConfig.KNN_RADIUS;
    }
    double deviation = 1e-6;
    double left = 0;
    int loop = 0;
    int k = getK(filter, fetch);
    long count = 0L;
    if (USE_DP) {
      while (left + deviation <= right) {
        double mid = (left + right) / 2;
        LOG.debug("k: {} left: {} right: {} mid: {}", k, left, right, mid);
        Pair<Double, Double> res = dPRangeCount(
            FederateService.PrivacyCountRequest.newBuilder().setCacheUuid(uuid).setRadius(mid).setUuid(uuid).build(),
            tableClients);
        count = Math.round(res.getKey());
        if (Math.abs(res.getKey() - k) < res.getValue()) {
          LOG.debug("change method on loop {}", loop);
          break;
        }
        if (count > k) {
          right = mid;
        } else if (count < k) {
          left = mid;
        }
        loop++;
        LOG.debug("loop {} with result size {}", loop, count);
      }
    }
    while (left + deviation <= right) {
      double mid = (left + right) / 2;
      PrivacyCompareRequest.Builder request = PrivacyCompareRequest.newBuilder().setCacheid(uuid).setRadius(mid);
      int sign = privacyCompare(request, tableClients, k);
      LOG.debug("loop {} with  sign {}", loop, sign);
      if (sign < 0) {
        left = mid;
      } else if (sign > 0) {
        right = mid;
      } else {
        loop++;
        try {
          return knnCircleRangeQuery(query.clone(), tableClients, uuid, mid);
        } finally {
          clearCache(uuid, tableClients);
        }
      }
      loop++;
    }
    LOG.warn("loop {} with approximate result size {}", loop);
    try {
      return knnCircleRangeQuery(query.clone(), tableClients, uuid, right);
    } finally {
      clearCache(uuid, tableClients);
    }
  }

  // private StreamingIterator<DataSetProto> privacyKnn(Header header,
  // List<Expression> project,
  // List<Expression> filter, Map<FederateDBClient, String> tableClients, int
  // fetch, List<String> order) {
  // String uuid = UUID.randomUUID().toString();
  // Query.Builder query = Query.newBuilder().setHeader(header.toProto())
  // .addAllProjectExp(project).addAllFilterExp(filter).setFetch(fetch).addAllOrder(order);
  // double right = knnRadiusQuery(tableClients, query.clone(), uuid) * 2;
  // if (!USE_DP) {
  // right = FedSpatialConfig.KNN_RADIUS;
  // }
  // double deviation = 1e-6;
  // double left = 0;
  // int loop = 0;
  // int k = query.getFetch();
  // long count = 0L;
  // boolean acc = !USE_DP;
  // while (left + deviation <= right) {
  // double mid = (left + right) / 2;
  // LOG.debug("k: {} left: {} right: {} mid: {}", k, left, right, mid);
  // if (!acc) {
  // Pair<Double, Double> res =
  // dPRangeCount(FederateService.PrivacyCountRequest.newBuilder().setCacheUuid(uuid).setRadius(mid).setUuid(uuid).build(),
  // tableClients);
  // count = Math.round(res.getKey());
  // if (Math.abs(res.getKey() - k) < res.getValue()) {
  // LOG.warn("change method on loop {}", loop);
  // acc = true;
  // }
  // }
  // if (acc) {
  // double result = ShamirSharing.shamirCount(tableClients,
  // FederateService.PrivacyCountRequest.newBuilder().setCacheUuid(uuid).setRadius(mid),
  // executorService);
  // LOG.warn("loop {} with result {}", loop, result);
  // count = Math.round(result);
  // }
  // if (count > k) {
  // right = mid;
  // } else if (count < k) {
  // left = mid;
  // } else {
  // loop++;
  // LOG.debug("loop {} with result size {}", loop, count);
  // try {
  // return knnCircleRangeQuery(query.clone(), tableClients, uuid, mid);
  // } finally {
  // clearCache(uuid, tableClients);
  // }
  // }
  // loop++;
  // LOG.debug("loop {} with result size {}", loop, count);
  // }
  // LOG.warn("loop {} with approximate result size {}", loop, count);
  // try {
  // return knnCircleRangeQuery(query.clone(), tableClients, uuid, right);
  // } finally {
  // clearCache(uuid, tableClients);
  // }
  // }

  private DataSet calculateAgg(String aggUuid, DataSet localSet,
      List<Map.Entry<AggregateType, List<Integer>>> aggregateFields, Header rawHeader,
      Map<FederateDBClient, String> tableClients) {
    final int size = aggregateFields.size();
    List<FederateFieldType> types = new ArrayList<>(size);
    int i = 0;
    int typeIdx = 0;
    List<AggregateFunc> funcs = new ArrayList<>(size);
    boolean[] hide = new boolean[size];
    for (Map.Entry<AggregateType, List<Integer>> entry : aggregateFields) {
      AggregateType aggType = entry.getKey();
      List<Integer> columns = entry.getValue();
      hide[i] = rawHeader.getLevel(columns.get(0)) == Level.HIDE;
      FederateFieldType type = localSet.getType(typeIdx);
      types.add(type);
      funcs.add(AggregateFuncImpl.getAggFunc(aggType, type, columns));
      ++i;
      typeIdx += columns.size();
    }
    Header.IteratorBuilder builder = Header.newBuilder();
    for (FederateFieldType type : types) {
      builder.add("", type);
    }
    Header header = builder.build();
    DataSet result = DataSet.newDataSet(header);
    for (int j = 0; j < size; j++) {
      if (hide[j]) {
        funcs.get(j).ShamirCount(aggUuid, tableClients, executorService);
      } else {
        for (DataSet.DataRow row : localSet) {
          funcs.get(j).addRow(row);
        }
      }
    }
    clearCache(aggUuid, tableClients);
    DataSet.DataRowBuilder rowBuilder = result.newRow();
    for (int j = 0; j < size; ++j) {
      rowBuilder.set(j, funcs.get(j).result());
    }
    rowBuilder.build();
    return result;
  }
}
