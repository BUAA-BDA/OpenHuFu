package tk.onedb.core.zk;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tk.onedb.core.config.OneDBConfig;

public abstract class ZkClient implements Watcher {
  protected static Logger LOG = LoggerFactory.getLogger(ZkClient.class);

  protected String zkRootPath;
  protected String endpointRootPath;
  protected ZooKeeper zk;

  public ZkClient(String servers, String zkRootPath) throws IOException {
    this.zkRootPath = zkRootPath;
    this.endpointRootPath = zkRootPath + "/endpoint";
    this.zk = new ZooKeeper(servers, OneDBConfig.ZK_TIME_OUT, this);
  }

  protected void initRootPath() throws KeeperException, InterruptedException {
    LOG.info("root path: {}; endpoint path: {}", zkRootPath, endpointRootPath);
    if (zk.exists(zkRootPath, false) == null) {
      LOG.info("init root path: {}", zkRootPath);
      zk.create(zkRootPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      LOG.info("init endpoint path: {}", endpointRootPath);
      zk.create(endpointRootPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } else if (zk.exists(endpointRootPath, false) == null) {
      LOG.info("init endpoint path: {}", endpointRootPath);
      zk.create(endpointRootPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  protected static String buildPath(String rootPath, String nodeName) {
    return rootPath + "/" + nodeName;
  }

  @Override
  public void process(WatchedEvent event) {
    switch(event.getState()) {
      case SyncConnected:
        break;
      case Expired:
        LOG.error("ZK connection expired");
        break;
      default:
        LOG.error("ZK status {}", event.getState());
    }
  }
}