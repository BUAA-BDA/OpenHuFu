package com.hufudb.openhufu.core.zk;

import com.hufudb.openhufu.core.config.FQConfig;
import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkClient implements Watcher {
  protected static Logger LOG = LoggerFactory.getLogger(ZkClient.class);

  protected String zkRootPath;
  protected String endpointRootPath;
  protected String schemaRootPath;
  protected ZooKeeper zk;

  public ZkClient(String servers, String zkRootPath) {
    this.zkRootPath = zkRootPath;
    this.endpointRootPath = zkRootPath + "/endpoint";
    this.schemaRootPath = zkRootPath + "/schema";
    try {
      this.zk = new ZooKeeper(servers, FQConfig.ZK_TIME_OUT, this);
      initRootPath();
    } catch (IOException e) {
      LOG.error("Error when init ZkClient", e);
    }
  }

  protected static String buildPath(String rootPath, String nodeName) {
    return rootPath + "/" + nodeName;
  }

  protected void initRootPath() {
    LOG.info("root path: {}; endpoint path: {}", zkRootPath, endpointRootPath);
    createRecursive(endpointRootPath, Ids.OPEN_ACL_UNSAFE);
    createRecursive(schemaRootPath, Ids.OPEN_ACL_UNSAFE);
  }

  protected boolean createRecursive(String path, List<ACL> acls) {
    int idx = 0;
    LOG.info("Creating path {} recursively", path);
    while (true) {
      idx = path.indexOf("/", idx + 1);
      String tmp = idx == -1 ? path : path.substring(0, idx);
      try {
        if (zk.exists(tmp, false) == null) {
          zk.create(tmp, null, acls, CreateMode.PERSISTENT);
          LOG.info("Create path {}", tmp);
        }
      } catch (KeeperException | InterruptedException e) {
        LOG.error("Error when creating path {}", tmp, e);
        Thread.currentThread().interrupt();
        return false;
      }
      if (idx == -1) {
        break;
      }
    }
    return true;
  }

  @Override
  public void process(WatchedEvent event) {
    switch (event.getState()) {
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
