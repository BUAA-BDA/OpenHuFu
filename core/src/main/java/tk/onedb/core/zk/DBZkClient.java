package tk.onedb.core.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import tk.onedb.core.config.OneDBConfig;
import tk.onedb.core.data.TableInfo;

public class DBZkClient extends ZkClient {
  private final String dbRootPath;
  private List<ACL> DB_AUTH;

  public DBZkClient(String servers, String zkRootPath, String endpoint, byte[] digest) throws IOException, KeeperException, InterruptedException {
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
      zk.create(dbRootPath, null, DB_AUTH, CreateMode.PERSISTENT);
    } else {
      LOG.info("DBPath {} already exists", dbRootPath);
    }
  }

  public boolean addTableInfo(TableInfo tableInfo) {
    String tablePath = buildPath(dbRootPath, tableInfo.getName());
    byte[] header = tableInfo.getHeader().toProto().toByteArray();
    try {
      zk.create(tablePath, header, DB_AUTH, CreateMode.EPHEMERAL);
      LOG.info("Add table info on path {} success", tablePath);
      return true;
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Fail to add table info on path {}: {}", tablePath, e.getMessage());
      return false;
    }
  }

  public boolean registerTable2Schema(String schema, String globalTableName, String endpoint, String localTableName) {
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

  public boolean deleteTableInfo(String name) {
    String tablePath = buildPath(dbRootPath, name);
    try {
      zk.delete(tablePath, -1);
      LOG.info("delete table info on path {} success", tablePath);
      return true;
    } catch (KeeperException e) {
      LOG.error("Error when delete node [{}]: {}", tablePath, e.getMessage());
      return false;
    } catch (InterruptedException e) {
      LOG.error("Interrupted when delete node [{}]: {}", tablePath, e.getMessage());
      return false;
    }
  }
}
