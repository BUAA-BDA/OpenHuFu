package com.hufudb.onedb.core.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.protobuf.Empty;
import com.hufudb.onedb.core.config.OneDBConfig;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.StreamBuffer;
import com.hufudb.onedb.core.sql.enumerator.RowEnumerator;
import com.hufudb.onedb.core.sql.expression.OneDBQuery;
import com.hufudb.onedb.core.sql.rel.OneDBQueryContext;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.core.utils.EmptyEnumerator;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;

/*
* client for all DB
*/
public class OneDBClient {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBClient.class);

  private final OneDBSchema schema;
  private final Map<String, OwnerClient> ownerMap;
  private final Map<String, OneDBTableInfo> tableMap;
  private final ExecutorService executorService;


  public OneDBClient(OneDBSchema schema) {
    this.schema = schema;
    ownerMap = new ConcurrentHashMap<>();
    tableMap = new ConcurrentHashMap<>();
    this.executorService = Executors.newFixedThreadPool(OneDBConfig.CLIENT_THREAD_NUM);
  }


  Map<String, OneDBTableInfo> getTableMap() {
    return tableMap;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public OwnerClient addOwner(String endpoint) {
    if (hasOwner(endpoint)) {
      LOG.info("DB at {} already exists", endpoint);
      return getOwnerClient(endpoint);
    }
    OwnerClient client = new OwnerClient(endpoint);
    if (client != null) {
      ownerMap.put(endpoint, client);
    }
    LOG.info("add DB {}", endpoint);
    return client;
  }

  public boolean hasOwner(String endpoint) {
    return ownerMap.containsKey(endpoint);
  }

  public OwnerClient getOwnerClient(String endpoint) {
    return ownerMap.get(endpoint);
  }

  public Set<String> getEndpoints() {
    return ownerMap.keySet();
  }

  // add global table through zk
  public void addTable2Schema(String tableName, Table table) {
    schema.addTable(tableName, table);
  }

  // add global table through model.json
  public void addTable(String tableName, OneDBTableInfo table) {
    this.tableMap.put(tableName, table);
  }

  // drop global table
  public void dropTable(String tableName) {
    tableMap.remove(tableName);
  }

  public OneDBTableInfo getTable(String tableName) {
    return tableMap.get(tableName);
  }

  public boolean hasTable(String tableName) {
    return tableMap.containsKey(tableName);
  }

  // for local table
  public void removeLocalTable(String globalTableName, String endpoint, String localTableName) {
    OneDBTableInfo table = getTable(globalTableName);
    if (table == null) {
      LOG.error("Gloabl table {} not exists", globalTableName);
      return;
    }
    OwnerClient client = getOwnerClient(endpoint);
    if (client == null) {
      LOG.error("Endpoint {} not exists", endpoint);
    }
    table.dropLocalTable(client, localTableName);
  }

  public void removeLocalTable(String endpoint, String tableName) {
    for (OneDBTableInfo info : tableMap.values()) {
      info.dropLocalTable(ownerMap.get(endpoint), tableName);
    }
  }

  public void removeOwner(String endpoint) {
    OwnerClient client = ownerMap.remove(endpoint);
    for (OneDBTableInfo info : tableMap.values()) {
      info.removeOwner(client);
    }
  }

  public Header getHeader(String tableName) {
    OneDBTableInfo table = getTable(tableName);
    return table != null ? table.getHeader() : null;
  }

  public List<Pair<OwnerClient, String>> getTableClients(String tableName) {
    OneDBTableInfo table = getTable(tableName);
    return table != null ? table.getTableList() : null;
  }

  /*
  * onedb query
  */
  public Enumerator<Row> oneDBQuery(String tableName, OneDBQueryProto query) {
    List<Pair<OwnerClient, String>> tableClients = getTableClients(tableName);
    StreamBuffer<DataSetProto> streamProto = oneDBQuery(query, tableClients);
    Header header = OneDBQuery.generateHeader(query);
    if (query.getAggExpCount() > 0) {
      BasicDataSet localDataSet = BasicDataSet.of(header);
      while (streamProto.hasNext()) {
        localDataSet.mergeDataSet(BasicDataSet.fromProto(streamProto.next()));
      }
      return localDataSet;
    } else {
      return new RowEnumerator(streamProto, query.getFetch());
    }
  }

  // todo: execute query in this function
  public Enumerator<Object> oneDBQuery(Long contextId) {
    OneDBQueryContext context = OneDBQueryContext.getContext(contextId);
    LOG.info("execute query id[{}]: {}", contextId, context.toProtoStr());
    return new EmptyEnumerator<>();
  }

  private StreamBuffer<DataSetProto> oneDBQuery(OneDBQueryProto query, List<Pair<OwnerClient, String>> tableClients) {
    StreamBuffer<DataSetProto> iterator = new StreamBuffer<>(tableClients.size());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (Pair<OwnerClient, String> entry : tableClients) {
      tasks.add(() -> {
        try {
          OneDBQueryProto localQuery = query.toBuilder().setTableName(entry.getValue()).build();
          Iterator<DataSetProto> it = entry.getKey().oneDBQuery(localQuery);
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
          LOG.error("error in oneDBQuery");
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error in OneDBQuery for {}", e.getMessage());
    }
    return iterator;
  }
}
