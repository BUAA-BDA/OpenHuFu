package group.bda.federate.sql.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import group.bda.federate.sql.table.FederateTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.data.Header;
import group.bda.federate.client.FedSpatialClient;
import group.bda.federate.client.FederateDBClient;

public class FederateSchema extends AbstractSchema {
  private static final Logger LOG = LogManager.getLogger(FederateSchema.class);

  private final Map<String, Table> tableMap;
  private final FedSpatialClient client;


  public FederateSchema(List<String> endpoints, List<Map<String, Object>> tables) {
    LOG.info("add schema");
    this.tableMap = new HashMap<>();
    this.client = new FedSpatialClient();
    for (String endpoint : endpoints) {
      if (!addFederate(endpoint)) {
        LOG.warn("fed {} already exist", endpoint);
      } else {
        LOG.info("add fed {}", endpoint);
      }
    }
  }

  public FederateSchema(List<String> endpoints) {
    this(endpoints, ImmutableList.of());
  }

  public FedSpatialClient getClient() {
    return client;
  }

  public boolean addFederate(String endpoint) {
    return client.addFederate(endpoint);
  }

  public boolean hasFederate(String endpoint) {
    return client.hasFederate(endpoint);
  }

  public void addTable(String tableName, Table table) {
    client.addTable(tableName, ((FederateTable) table).getTableInfo());
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

  public Header generateHeader(String tableName, List<String> columns) {
    return client.generateHeader(tableName, columns);
  }

  public FederateDBClient getDBClient(String endpoint) {
    return client.getDBClient(endpoint);
  }

  public Map<FederateDBClient, String> getClients(String tableName) {
    FederateTable table = (FederateTable) tableMap.get(tableName);
    if (table == null) {
      return ImmutableMap.of();
    }
    return table.getTableInfo().getTableMap();
  }

  @Deprecated
  public ExecutorService getExecutorService() {
    return client.getExecutorService();
  }
}
