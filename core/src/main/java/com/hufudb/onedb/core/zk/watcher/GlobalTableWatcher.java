package com.hufudb.onedb.core.zk.watcher;

import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.core.table.OneDBTableSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/*
 * Watch the number of local table of specific global table
 * dynamically add and remove local table according to zk
 */
public class GlobalTableWatcher extends ZkWatcher {
  private final OneDBTableSchema tableInfo;

  public GlobalTableWatcher(OneDBSchema schema, ZooKeeper zk, String path) {
    super(schema, zk, path);
    String[] dic = path.split("/");
    String tableName = dic[dic.length - 1];
    this.tableInfo = schema.getOneDBTableInfo(tableName);
  }

  private void watchLocalTableChange(List<String> newEndpoints) {
    List<String> addedTable = new ArrayList<>();
    List<String> droppedTable = new ArrayList<>();
    List<String> oldEndpoint = tableInfo.getEndpoints();
    for (String endpoint : newEndpoints) {
      if (oldEndpoint.indexOf(endpoint) == -1) {
        addedTable.add(endpoint);
      }
    }
    for (String endpoint : oldEndpoint) {
      if (newEndpoints.indexOf(endpoint) == -1) {
        droppedTable.add(endpoint);
      }
    }
    addTable(addedTable);
    dropTable(droppedTable);
  }

  private void addTable(List<String> endpoints) {
    for (String endpoint : endpoints) {
      String localPath = buildPath(path, endpoint);
      try {
        String localName =
            new String(zk.getData(localPath, new LocalTableWatcher(schema, zk, localPath), null));
        schema.addLocalTable(tableInfo.getName(), endpoint, localName);
      } catch (Exception e) {
        LOG.error("Error when get Local Table in {} : {}", endpoint, e.getMessage());
      }
    }
  }

  private void dropTable(List<String> endpoints) {
    for (String endpoint : endpoints) {
      schema.dropLocalTable(tableInfo.getName(), endpoint);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    EventType type = event.getType();
    KeeperState state = event.getState();
    if (!state.equals(KeeperState.SyncConnected)) {
      return;
    }
    String path = event.getPath();
    switch (type) {
      case NodeChildrenChanged:
        synchronized (this) {
          try {
            List<String> children = zk.getChildren(path, this);
            watchLocalTableChange(children);
          } catch (Exception e) {
            LOG.error("Error in global table watcher on path {} : {}", path, e.getMessage());
          }
        }
        break;
      case NodeDeleted:
        LOG.info("Node {} has been deleted", path);
      default:
        break;
    }
  }
}
