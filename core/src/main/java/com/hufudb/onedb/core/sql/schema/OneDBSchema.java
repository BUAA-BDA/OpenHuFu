package com.hufudb.onedb.core.sql.schema;

import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.enumerator.OneDBEnumerator;
import com.hufudb.onedb.core.sql.rel.OneDBTable;
import com.hufudb.onedb.core.sql.schema.OneDBSchemaFactory.OwnerMeta;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.core.zk.OneDBZkClient;
import com.hufudb.onedb.core.zk.ZkConfig;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.grpc.ChannelCredentials;
import io.grpc.TlsChannelCredentials;

public class OneDBSchema extends AbstractSchema {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBSchema.class);

  private final SchemaPlus parentSchema;
  private final Map<String, Table> tableMap;
  private final OneDBClient client;
  private final OneDBZkClient zkClient;
  private final int userId;
  private final AtomicInteger queryCounter;

  public OneDBSchema(List<Map<String, Object>> tables, SchemaPlus schema, ZkConfig zkConfig) {
    this.parentSchema = schema;
    this.tableMap = new HashMap<>();
    this.client = new OneDBClient(this);
    this.zkClient = new OneDBZkClient(zkConfig, this);
    this.userId = 0;
    this.queryCounter = new AtomicInteger(0);
  }

  public OneDBSchema(List<OwnerMeta> owners, List<Map<String, Object>> tables, SchemaPlus schema, int userId) {
    this.parentSchema = schema;
    this.tableMap = new HashMap<>();
    this.client = new OneDBClient(this);
    this.zkClient = null;
    this.userId = userId;
    this.queryCounter = new AtomicInteger(0);
    for (OwnerMeta owner : owners) {
      addOwner(owner.getEndpoint(), owner.getTrustCertPath());
    }
  }

  public OneDBClient getClient() {
    return client;
  }

  public Set<String> getEndpoints() {
    return client.getEndpoints();
  }

  public OneDBTableInfo getOneDBTableInfo(String tableName) {
    return ((OneDBTable) getTable(tableName)).getTableInfo();
  }

  public List<OneDBTableInfo> getAllOneDBTableInfo() {
    List<OneDBTableInfo> infos = new ArrayList<>();
    for (Table table : tableMap.values()) {
      infos.add(((OneDBTable) table).getTableInfo());
    }
    return infos;
  }

  public int getUserId() {
    return userId;
  }

  public int getAndIncrementQueryCounter() {
    return queryCounter.getAndIncrement();
  }

  public long stampQueryId() {
    return ((long) getUserId() << 32) | (long) getAndIncrementQueryCounter();
  }

  public OwnerClient addOwner(String endpoint, String trustCertPath) {
    try {
      ChannelCredentials cred = null;
      if (trustCertPath != null) {
        File trustCertFile = new File(trustCertPath);
        cred = TlsChannelCredentials.newBuilder().trustManager(trustCertFile).build();
      }
      return client.addOwner(endpoint, cred);
    } catch (IOException e) {
      LOG.error("Fail to create channel credentials: {}", e.getMessage());
      return null;
    }
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

  public Expression getExpression() {
    return Schemas.unwrap(super.getExpression(parentSchema, "onedb"), OneDBSchema.class);
  }

  @SuppressWarnings("unused")
  public Enumerable<Object> query(long contextId) {
    return new AbstractEnumerable<Object>() {
      Enumerator<Object> enumerator;

      @Override
      public Enumerator<Object> enumerator() {
        if (enumerator == null) {
          this.enumerator = new OneDBEnumerator(OneDBSchema.this, contextId);

        } else {
          this.enumerator.reset();
        }
        return this.enumerator;
      }
    };
  }
}
