package com.hufudb.onedb.core.client;

import com.hufudb.onedb.core.config.OneDBConfig;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.UserSideImplementor;
import com.hufudb.onedb.core.rewriter.BasicRewriter;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBQueryContextPool;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.grpc.ChannelCredentials;
import org.apache.commons.lang3.tuple.Pair;

/*
 * client for all DB
 */
public class OneDBClient {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBClient.class);
  private final OneDBSchema schema;
  private final Map<String, OwnerClient> ownerMap;
  private final Map<String, OneDBTableInfo> tableMap;
  final ExecutorService threadPool;
  private final AtomicInteger queryId;

  public OneDBClient(OneDBSchema schema) {
    this.schema = schema;
    this.ownerMap = new ConcurrentHashMap<>();
    this.tableMap = new ConcurrentHashMap<>();
    this.threadPool = Executors.newFixedThreadPool(OneDBConfig.CLIENT_THREAD_NUM);
    this.queryId = new AtomicInteger(0);
  }

  int getQueryId() {
    return queryId.getAndIncrement();
  }

  int getQueryId(int offset) {
    return queryId.getAndAdd(offset);
  }

  public long getTaskId() {
    return (((long) schema.getUserId()) << 32) | (long) getQueryId();
  }

  public long getTaskId(int offset) {
    return (((long) schema.getUserId()) << 32) | (long) getQueryId(offset);
  }

  Map<String, OneDBTableInfo> getTableMap() {
    return tableMap;
  }

  public ExecutorService getThreadPool() {
    return threadPool;
  }

  public OwnerClient addOwner(String endpoint, ChannelCredentials cred) {
    if (hasOwner(endpoint)) {
      LOG.info("Owner at {} already exists", endpoint);
      return getOwnerClient(endpoint);
    }
    OwnerClient client = null;
    try {
      if (cred != null) {
        client = new OwnerClient(endpoint, cred);
      } else {
        client = new OwnerClient(endpoint);
      }
      if (client != null) {
        // establish connection among owners
        for (Map.Entry<String, OwnerClient> entry : ownerMap.entrySet()) {
          OwnerClient oldClient = entry.getValue();
          oldClient.addOwner(client.getParty());
          client.addOwner(oldClient.getParty());
        }
        ownerMap.put(endpoint, client);
      }
      LOG.info("Add owner {}", endpoint);
    } catch (Exception e) {
      LOG.warn("Fail to add owner {}: {}", endpoint, e.getMessage());
    }
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
  public Enumerator<Row> oneDBQuery(long contextId) {
    OneDBContext context = OneDBQueryContextPool.getContext(contextId);
    // todo: support for choosing the appropritate rewriter
    context = context.rewrite(new BasicRewriter());
    return UserSideImplementor.getImplementor(context, this).implement(context);
  }
}
