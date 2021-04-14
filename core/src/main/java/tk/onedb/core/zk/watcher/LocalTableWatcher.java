package tk.onedb.core.zk.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import tk.onedb.core.sql.schema.OneDBSchema;

public class LocalTableWatcher extends ZkWatcher {
  private String endpoint;
  private String tableName;

  public LocalTableWatcher(OneDBSchema schema, ZooKeeper zk, String path) {
    super(schema, zk, path);
    String dic[] = path.split("/");
    this.endpoint = dic[dic.length - 1];
    this.tableName = dic[dic.length - 2];
  }

  public void changeLocalTable(String localTableName) {
    schema.changeLocalTable(tableName, endpoint, localTableName);
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
      case NodeDataChanged:
        try {
          String tableName = new String(zk.getData(path, this, null));
          changeLocalTable(tableName);
        } catch (Exception e) {
          LOG.error("Error in local table watcher on path {} : {}", path, e.getMessage());
        }
        break;
      case NodeDeleted:
        LOG.info("Node {} has been deleted", path);
      default:
        break;
    }
  }
}
