package tk.onedb.core.zk.watcher;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tk.onedb.core.client.OneDBClient;

public abstract class ZkWatcher implements Watcher {
  protected static Logger LOG = LoggerFactory.getLogger(ZkWatcher.class);

  protected final ZooKeeper zk;
  protected final OneDBClient client;
  protected final String path;

  public ZkWatcher(OneDBClient client, ZooKeeper zk, String path) {
    this.client = client;
    this.zk = zk;
    this.path = path;
  }
}