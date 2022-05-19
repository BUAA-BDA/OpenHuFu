package com.hufudb.onedb.core.zk;

import com.hufudb.onedb.core.sql.rel.OneDBTable;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.zk.watcher.EndpointWatcher;
import com.hufudb.onedb.core.zk.watcher.GlobalTableWatcher;
import com.hufudb.onedb.core.zk.watcher.LocalTableWatcher;
import java.util.List;
import org.apache.calcite.schema.Table;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class OneDBZkClient extends ZkClient {

  private final OneDBSchema schema;
  private final String schemaDirectoryPath;

  public OneDBZkClient(ZkConfig zkConfig, OneDBSchema schema) {
    super(zkConfig.servers, zkConfig.zkRoot);
    this.schemaDirectoryPath = buildPath(schemaRootPath, zkConfig.schemaName);
    this.schema = schema;
    loadZkTable();
  }

  public void loadZkTable() {
    try {
      watchEndpoints();
      watchSchemaDirectory();
    } catch (KeeperException | InterruptedException e) {
      e.printStackTrace();
    }
    LOG.info("Load table from zk success");
  }

  private void watchEndpoints() throws KeeperException, InterruptedException {
    List<String> endpoints =
        zk.getChildren(endpointRootPath, new EndpointWatcher(schema, zk, endpointRootPath));
    for (String endpoint : endpoints) {
      schema.addOwner(endpoint, null);
    }
  }

  private void watchSchemaDirectory() throws KeeperException, InterruptedException {
    if (zk.exists(schemaDirectoryPath, false) == null) {
      LOG.info("Create Schema Directory: {}", schemaDirectoryPath);
      zk.create(schemaDirectoryPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } else {
      LOG.info("Schema Directory {} already exists", schemaDirectoryPath);
    }
    List<String> globalTables = zk.getChildren(schemaDirectoryPath, false);
    for (String globalTable : globalTables) {
      watchGlobalTable(globalTable);
    }
  }

  public void watchGlobalTable(String tableName) throws KeeperException, InterruptedException {
    String gPath = buildPath(schemaDirectoryPath, tableName);
    List<String> endpoints = zk.getChildren(gPath, null);
    GlobalTableConfig tableMeta = new GlobalTableConfig(tableName);
    for (String endpoint : endpoints) {
      schema.addOwner(endpoint, null);
      String localTableName = watchLocalTable(buildPath(gPath, endpoint));
      tableMeta.addLocalTable(endpoint, localTableName);
    }
    Table table = OneDBTable.create(schema, tableMeta);
    if (table != null) {
      schema.addTable(tableName, table);
      zk.getChildren(gPath, new GlobalTableWatcher(schema, zk, gPath));
    }
  }

  private String watchLocalTable(String lPath) throws KeeperException, InterruptedException {
    return new String(zk.getData(lPath, new LocalTableWatcher(schema, zk, lPath), null));
  }
}
