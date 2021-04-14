package tk.onedb.core.zk.watcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import tk.onedb.core.sql.schema.OneDBSchema;
import tk.onedb.core.zk.OneDBZkClient;

public class SchemaWatcher extends ZkWatcher {
  private OneDBZkClient zkClient;

  public SchemaWatcher(OneDBSchema schema, ZooKeeper zk, String path, OneDBZkClient zkClient) {
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
      } catch (Exception e) {
        LOG.error("Error when watching global table {} : {}", table, e.getMessage());
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
        synchronized(this) {
          try {
            List<String> children = zk.getChildren(path, this);
            watchGlobalTableChange(children);
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
