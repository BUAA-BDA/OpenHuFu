package com.hufudb.onedb.core.zk.watcher;

import com.hufudb.onedb.core.sql.schema.OneDBSchemaManager;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkWatcher implements Watcher {
  protected static Logger LOG = LoggerFactory.getLogger(ZkWatcher.class);

  protected final ZooKeeper zk;
  protected final OneDBSchemaManager schema;
  protected final String path;

  public ZkWatcher(OneDBSchemaManager schema, ZooKeeper zk, String path) {
    this.schema = schema;
    this.zk = zk;
    this.path = path;
  }

  protected static String buildPath(String rootPath, String nodeName) {
    return rootPath + "/" + nodeName;
  }
}
