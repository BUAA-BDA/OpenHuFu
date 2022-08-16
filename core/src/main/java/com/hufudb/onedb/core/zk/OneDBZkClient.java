package com.hufudb.onedb.core.zk;

import com.hufudb.onedb.core.sql.rel.OneDBTable;
import com.hufudb.onedb.core.sql.schema.OneDBSchemaManager;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.table.LocalTableConfig;
import com.hufudb.onedb.core.zk.watcher.EndpointWatcher;
import com.hufudb.onedb.core.zk.watcher.GlobalTableWatcher;
import com.hufudb.onedb.core.zk.watcher.LocalTableWatcher;
import java.util.List;
import org.apache.calcite.schema.Table;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class OneDBZkClient extends ZkClient {

  private final OneDBSchemaManager manager;
  private final String schemaDirectoryPath;

  public OneDBZkClient(ZkConfig zkConfig, OneDBSchemaManager manager) {
    super(zkConfig.servers, zkConfig.zkRoot);
    this.schemaDirectoryPath = buildPath(schemaRootPath, zkConfig.schemaName);
    this.manager = manager;
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
        zk.getChildren(endpointRootPath, new EndpointWatcher(manager, zk, endpointRootPath));
    for (String endpoint : endpoints) {
      manager.addOwner(endpoint, null);
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
      manager.addOwner(endpoint, null);
      String localTableName = watchLocalTable(buildPath(gPath, endpoint));
      tableMeta.localTables.add(new LocalTableConfig(endpoint, localTableName));
    }
    Table table = OneDBTable.create(manager, tableMeta);
    if (table != null) {
      manager.addTable(tableName, table);
      zk.getChildren(gPath, new GlobalTableWatcher(manager, zk, gPath));
    }
  }

  private String watchLocalTable(String lPath) throws KeeperException, InterruptedException {
    return new String(zk.getData(lPath, new LocalTableWatcher(manager, zk, lPath), null));
  }
}
