package com.hufudb.openhufu.core.zk.watcher;

import com.hufudb.openhufu.core.sql.schema.FQSchemaManager;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkWatcher implements Watcher {
  protected static Logger LOG = LoggerFactory.getLogger(ZkWatcher.class);

  protected final ZooKeeper zk;
  protected final FQSchemaManager schema;
  protected final String path;

  public ZkWatcher(FQSchemaManager schema, ZooKeeper zk, String path) {
    this.schema = schema;
    this.zk = zk;
    this.path = path;
  }

  protected static String buildPath(String rootPath, String nodeName) {
    return rootPath + "/" + nodeName;
  }
}
