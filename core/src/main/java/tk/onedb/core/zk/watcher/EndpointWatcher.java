package tk.onedb.core.zk.watcher;


import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import tk.onedb.core.client.OneDBClient;

public class EndpointWatcher extends ZkWatcher {

  public EndpointWatcher(OneDBClient client, ZooKeeper zk, String path) {
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
      switch (type) {
        case NodeChildrenChanged:
          List<String> children = zk.getChildren(path, new EndpointWatcher(client, zk, path));
          LOG.info("Change on zk path {}: {}", event.getPath(), children.toString());
        default:
          break;
      }
    } catch (Exception e) {
      LOG.error("Error when creating children watcher on endpoints");
    }
  }
}
