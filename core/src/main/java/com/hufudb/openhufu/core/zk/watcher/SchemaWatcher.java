package com.hufudb.openhufu.core.zk.watcher;

import com.hufudb.openhufu.core.sql.schema.FQSchemaManager;
import com.hufudb.openhufu.core.zk.FQZkClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class SchemaWatcher extends ZkWatcher {
  private final FQZkClient zkClient;

  public SchemaWatcher(FQSchemaManager schema, ZooKeeper zk, String path, FQZkClient zkClient) {
    super(schema, zk, path);
    this.zkClient = zkClient;
  }

  private void watchGlobalTableChange(List<String> newTables) {
    List<String> addedTable = new ArrayList<>();
    List<String> droppedTable = new ArrayList<>();
    Set<String> oldTables = schema.getTableNames();
    for (String endpoint : newTables) {
      if (!oldTables.contains(endpoint)) {
        addedTable.add(endpoint);
      }
    }
    for (String endpoint : oldTables) {
      if (newTables.indexOf(endpoint) == -1) {
        droppedTable.add(endpoint);
      }
    }
    addTable(addedTable);
    dropTable(droppedTable);
  }

  private void addTable(List<String> tables) {
    for (String table : tables) {
      try {
        zkClient.watchGlobalTable(table);
      } catch (KeeperException | InterruptedException e) {
        LOG.error("Error when watching global table {}", table, e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private void dropTable(List<String> tables) {
    for (String table : tables) {
      schema.dropTable(table);
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
            watchGlobalTableChange(children);
          } catch (KeeperException | InterruptedException e) {
            LOG.error("Error in global table watcher on path {}", path, e);
            Thread.currentThread().interrupt();
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
