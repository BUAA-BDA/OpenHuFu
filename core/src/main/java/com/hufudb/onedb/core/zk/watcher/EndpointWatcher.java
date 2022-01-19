package com.hufudb.onedb.core.zk.watcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import com.hufudb.onedb.core.sql.schema.OneDBSchema;

public class EndpointWatcher extends ZkWatcher {

  public EndpointWatcher(OneDBSchema schema, ZooKeeper zk, String path) {
    super(schema, zk, path);
  }

  private void watchEndpoint(List<String> newEndpoints) {
    List<String> addedEndpoint = new ArrayList<>();
    List<String> droppedEndpoint = new ArrayList<>();
    Set<String> oldEndpoint = schema.getEndpoints();
    for (String endpoint : newEndpoints) {
      if (!oldEndpoint.contains(endpoint)) {
        addedEndpoint.add(endpoint);
      }
    }
    for (String endpoint : oldEndpoint) {
      if (newEndpoints.indexOf(endpoint) == -1) {
        droppedEndpoint.add(endpoint);
      }
    }
    dropEndpoint(droppedEndpoint);
    addEndpoint(addedEndpoint);
  }

  private void addEndpoint(List<String> endpoints) {
    for (String endpoint : endpoints) {
      schema.addDB(endpoint);
    }
  }

  private void dropEndpoint(List<String> endpoints) {
    for (String endpoint : endpoints) {
      schema.dropDB(endpoint);
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
            watchEndpoint(children);
          } catch (Exception e) {
            LOG.error("Error in endpoint watcher on path {} : {}", path, e.getMessage());
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
