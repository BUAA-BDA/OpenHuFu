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
import tk.onedb.core.data.FieldType;
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
    initRootPath();
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
      LOG.info("add table info on path {} success", tablePath);
      return true;
    } catch (KeeperException | InterruptedException e) {
      LOG.error("add table info on path {} failed: {}", tablePath, e.getMessage());
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

  public static void main(String[] args) {
    try {
      TableInfo table1 = TableInfo.newBuilder().add("id", FieldType.LONG).add("name", FieldType.STRING).setTableName("user").build();
      DBZkClient client = new DBZkClient("localhost:12349", "/onedb", "localhost:12345", "DB1:DB1".getBytes());
      Thread.sleep(10000);
      client.addTableInfo(table1);
      Thread.sleep(1000);
      client.addTableInfo(table1);
      Thread.sleep(10000);
      client.deleteTableInfo(table1.getName());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
