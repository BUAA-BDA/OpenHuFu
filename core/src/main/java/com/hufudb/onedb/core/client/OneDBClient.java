package com.hufudb.onedb.core.client;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.sql.rel.OneDBImplementor;
import com.hufudb.onedb.core.sql.rel.OneDBQueryContext;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.core.utils.EmptyEnumerator;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
* client for all DB
*/
public class OneDBClient {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBClient.class);

  private final OneDBSchema schema;
  private final Map<String, OwnerClient> ownerMap;
  private final Map<String, OneDBTableInfo> tableMap;
  private final OneDBImplementor implementor;

  public OneDBClient(OneDBSchema schema) {
    this.schema = schema;
    ownerMap = new ConcurrentHashMap<>();
    tableMap = new ConcurrentHashMap<>();
    implementor = new OneDBImplementor(this);
  }

  Map<String, OneDBTableInfo> getTableMap() {
    return tableMap;
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
  // todo: execute query in this function
  public Enumerator<Row> oneDBQuery(long contextId) {
    OneDBQueryContext context = OneDBQueryContext.getContext(contextId);
    return implementor.implement(context.toProto());
    // LOG.info("execute query id[{}]: {}", contextId, context.toProtoStr());
    // return new EmptyEnumerator<>();
  }
}
