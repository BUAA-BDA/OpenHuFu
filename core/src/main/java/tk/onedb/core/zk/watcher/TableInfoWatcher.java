package tk.onedb.core.zk.watcher;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import tk.onedb.core.client.OneDBClient;

public class TableInfoWatcher extends ZkWatcher {

  public TableInfoWatcher(OneDBClient client, ZooKeeper zk, String path) {
    super(client, zk, path);
  }

  @Override
  public void process(WatchedEvent event) {
    EventType type = event.getType();
    KeeperState state = event.getState();
    if (!state.equals(KeeperState.SyncConnected)) {
      return;
    }
    try {
      String path = event.getPath();
      switch (type) {
        case NodeChildrenChanged:
          List<String> children = zk.getChildren(path, new EndpointWatcher(client, zk, path));
          LOG.info("Node Children Changed on zk path {}: {}", path, children.toString());
          break;
        case NodeDeleted:
          LOG.info("Node {} has been deleted", path);
          break;
        case NodeDataChanged:
          zk.getData(path, new TableInfoWatcher(client, zk, path), null);
          LOG.info("Node {} data has been changed", path);
        default:
          break;
      }
    } catch (Exception e) {
      LOG.error("Error when creating children watcher on endpoints");
    }
  }
}
