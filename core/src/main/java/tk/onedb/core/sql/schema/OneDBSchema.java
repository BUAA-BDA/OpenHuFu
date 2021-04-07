package tk.onedb.core.sql.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tk.onedb.core.client.DBClient;
import tk.onedb.core.client.OneDBClient;
import tk.onedb.core.data.Header;
import tk.onedb.core.sql.rel.OneDBTable;
import tk.onedb.core.zk.ZkConfig;

public class OneDBSchema extends AbstractSchema {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBSchema.class);

  private final Map<String, Table> tableMap;
  private final OneDBClient client;

  public OneDBSchema(List<String> endpoints, List<Map<String, Object>> tables, ZkConfig zkConfig) {
    LOG.info("add schema");
    this.tableMap = new HashMap<>();
    this.client = new OneDBClient(zkConfig);
    for (String endpoint : endpoints) {
      if (!addFederate(endpoint)) {
        LOG.warn("fed {} already exist", endpoint);
      } else {
        LOG.info("add fed {}", endpoint);
      }
    }
  }

  public OneDBSchema(List<String> endpoints, List<Map<String, Object>> tables) {
    LOG.info("add schema");
    this.tableMap = new HashMap<>();
    this.client = new OneDBClient();
    for (String endpoint : endpoints) {
      if (!addFederate(endpoint)) {
        LOG.warn("fed {} already exist", endpoint);
      } else {
        LOG.info("add fed {}", endpoint);
      }
    }
  }

  public OneDBSchema(List<String> endpoints) {
    this(endpoints, ImmutableList.of());
  }

  public OneDBClient getClient() {
    return client;
  }

  public boolean addFederate(String endpoint) {
    return client.addDB(endpoint);
  }

  public boolean hasFederate(String endpoint) {
    return client.hasDB(endpoint);
  }

  public void addTable(String tableName, Table table) {
    client.addTable(tableName, ((OneDBTable) table).getTableInfo());
    tableMap.put(tableName, table);
  }

  public void dropTable(String tableName) {
    client.dropTable(tableName);
    tableMap.remove(tableName);
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

  public DBClient getDBClient(String endpoint) {
    return client.getDBClient(endpoint);
  }

  @Deprecated
  public ExecutorService getExecutorService() {
    return client.getExecutorService();
  }
}

