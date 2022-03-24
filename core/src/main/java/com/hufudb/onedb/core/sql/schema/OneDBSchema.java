package com.hufudb.onedb.core.sql.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.rel.OneDBTable;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.core.zk.OneDBZkClient;
import com.hufudb.onedb.core.zk.ZkConfig;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneDBSchema extends AbstractSchema {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBSchema.class);

  private final SchemaPlus parentSchema;
  private final Map<String, Table> tableMap;
  private final OneDBClient client;
  private final OneDBZkClient zkClient;

  public OneDBSchema(List<Map<String, Object>> tables, SchemaPlus schema, ZkConfig zkConfig) {
    this.parentSchema = schema;
    this.tableMap = new HashMap<>();
    this.client = new OneDBClient(this);
    this.zkClient = new OneDBZkClient(zkConfig, this);
  }

  public OneDBSchema(List<String> endpoints, List<Map<String, Object>> tables, SchemaPlus schema) {
    this.parentSchema = schema;
    this.tableMap = new HashMap<>();
    this.client = new OneDBClient(this);
    this.zkClient = null;
    for (String endpoint : endpoints) {
      addOwner(endpoint);
    }
  }

  public OneDBClient getClient() {
    return client;
  }

  public Set<String> getEndpoints() {
    return client.getEndpoints();
  }

  public OneDBTableInfo getOneDBTableInfo(String tableName) {
    return ((OneDBTable)getTable(tableName)).getTableInfo();
  }

  public List<OneDBTableInfo> getAllOneDBTableInfo() {
    List<OneDBTableInfo> infos = new ArrayList<>();
    for (Table table : tableMap.values()) {
      infos.add(((OneDBTable)table).getTableInfo());
    }
    return infos;
  }

  public OwnerClient addOwner(String endpoint) {
    return client.addOwner(endpoint);
  }

  public boolean hasOwner(String endpoint) {
    return client.hasOwner(endpoint);
  }

  public void removeOnwer(String endpoint) {
    client.removeOwner(endpoint);
  }

  public void addTable(String tableName, Table table) {
    parentSchema.add(tableName, table);
    client.addTable(tableName, ((OneDBTable) table).getTableInfo());
    tableMap.put(tableName, table);
  }

  public void dropTable(String tableName) {
    client.dropTable(tableName);
    tableMap.remove(tableName);
  }

  public void addLocalTable(String tableName, String endpoint, String localTableName) {
    OneDBTableInfo table = getOneDBTableInfo(tableName);
    OwnerClient client = getDBClient(endpoint);
    if (table != null && client != null) {
      table.addLocalTable(client, localTableName);
    }
  }

  public void dropLocalTable(String tableName, String endpoint) {
    OneDBTableInfo table = getOneDBTableInfo(tableName);
    OwnerClient client = getDBClient(endpoint);
    if (table != null && client != null) {
      table.dropLocalTable(client);
    }
  }

  public void changeLocalTable(String tableName, String endpoint, String localTableName) {
    OneDBTableInfo table = getOneDBTableInfo(tableName);
    OwnerClient client = getDBClient(endpoint);
    if (table != null && client != null) {
      table.changeLocalTable(client, localTableName);
    }
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  public boolean hasTable(String tableName) {
    return client.hasTable(tableName);
  }

  public Header getHeader(String tableName) {
    return client.getHeader(tableName);
  }

  public OwnerClient getDBClient(String endpoint) {
    return client.getOwnerClient(endpoint);
  }

  @Deprecated
  public ExecutorService getExecutorService() {
    return client.getExecutorService();
  }
}