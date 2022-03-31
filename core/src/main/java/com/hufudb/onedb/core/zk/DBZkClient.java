package com.hufudb.onedb.core.zk;

import com.hufudb.onedb.core.config.OneDBConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public class DBZkClient extends ZkClient {
  private final String dbRootPath;
  private List<ACL> DB_AUTH;

  public DBZkClient(String servers, String zkRootPath, String endpoint, byte[] digest)
      throws IOException, KeeperException, InterruptedException {
    super(servers, zkRootPath);
    this.dbRootPath = buildPath(endpointRootPath, endpoint);
    this.zk = new ZooKeeper(servers, OneDBConfig.ZK_TIME_OUT, this, false);
    zk.addAuthInfo("digest", digest);
    DB_AUTH = new ArrayList<>();
    DB_AUTH.addAll(Ids.READ_ACL_UNSAFE);
    DB_AUTH.addAll(Ids.CREATOR_ALL_ACL);
    init(endpoint);
  }

  private void init(String endpoint) throws KeeperException, InterruptedException {
    if (zk.exists(dbRootPath, false) == null) {
      LOG.info("Create DBPath: {}", dbRootPath);
      zk.create(dbRootPath, null, DB_AUTH, CreateMode.EPHEMERAL);
    } else {
      LOG.info("DBPath {} already exists", dbRootPath);
    }
  }

  public boolean registerTable(
      String schema, String globalTableName, String endpoint, String localTableName) {
    String directory = buildPath(schemaRootPath, schema);
    String tablePath = buildPath(directory, globalTableName);
    String registerPath = buildPath(tablePath, endpoint);
    try {
      if (createRecursive(tablePath, Ids.OPEN_ACL_UNSAFE)) {
        zk.create(registerPath, localTableName.getBytes(), DB_AUTH, CreateMode.EPHEMERAL);
        LOG.info("register {} to {}/{}/{}", localTableName, schema, globalTableName, endpoint);
        return true;
      } else {
        return false;
      }
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Error when register to schema {}: {}", schema, e.getMessage());
      e.printStackTrace();
      return false;
    }
  }

  public void removeTable(String schema, String globalTableName, String endpoint) {
    String directory = buildPath(schemaRootPath, schema);
    String tablePath = buildPath(directory, globalTableName);
    String registerPath = buildPath(tablePath, endpoint);
    try {
      if (zk.exists(registerPath, false) != null) {
        zk.delete(registerPath, -1);
      }
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Error when remove {} from schema {}: {}", endpoint, schema, e.getMessage());
      e.printStackTrace();
    }
  }
}
