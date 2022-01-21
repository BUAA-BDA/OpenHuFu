package com.hufudb.onedb.server;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import io.grpc.stub.StreamObserver;
import com.hufudb.onedb.rpc.OneDBService.GeneralRequest;
import com.hufudb.onedb.rpc.OneDBService.GeneralResponse;
import com.hufudb.onedb.rpc.ServiceGrpc;
import com.hufudb.onedb.core.client.DBClient;
import com.hufudb.onedb.core.data.AliasTableInfo;
import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.Field;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.StreamObserverDataSet;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.data.VirtualTableInfo;
import com.hufudb.onedb.core.sql.expression.OneDBQuery;
import com.hufudb.onedb.core.zk.DBZkClient;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.HeaderProto;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableListProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;

@Service
public abstract class DBService extends ServiceGrpc.ServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(DBService.class);
  protected final Map<String, DBClient> dbClientMap; // endpoint -> rpc_client
  private final Map<String, TableInfo> localTableInfoMap; // localName -> localTableInfo
  private final ReadWriteLock localLock;
  private final Map<String, VirtualTableInfo> virtualTableInfoMap; // virtualName -> virtualTableInfo
  private final ReadWriteLock virtualLock;
  // private final ExecutorService executorService;
  private final DBZkClient zkClient;
  protected final String endpoint;

  public DBService(String zkServers, String zkRootPath, String endpoint, String digest) {
    this.dbClientMap = new HashMap<>();
    this.localTableInfoMap = new HashMap<>();
    this.virtualTableInfoMap = new HashMap<>();
    // this.executorService = Executors.newFixedThreadPool(OneDBConfig.SERVER_THREAD_NUM);
    this.localLock = new ReentrantReadWriteLock();
    this.virtualLock = new ReentrantReadWriteLock();
    this.endpoint = endpoint;
    if (zkServers == null || zkRootPath == null || digest == null) {
      zkClient = null;
    } else {
      DBZkClient client;
      try {
         client = new DBZkClient(zkServers, zkRootPath, endpoint, digest.getBytes());
      } catch (Exception e) {
        LOG.error("Error when init DBZkClient: {}", e.getMessage());
        client = null;
      }
      this.zkClient = client;
    }
  }

  @Override
  public void oneDBQuery(OneDBQueryProto request, StreamObserver<DataSetProto> responseObserver) {
    OneDBQuery query = OneDBQuery.fromProto(request);
    Header header = query.generateHeader();
    StreamObserverDataSet obDataSet = new StreamObserverDataSet(responseObserver, header);
    try {
      oneDBQueryInternal(query, obDataSet);
    } catch (SQLException e) {
      LOG.error("error when query table [{}]", request.getTableName());
      e.printStackTrace();
    }
    obDataSet.close();
  }

  @Override
  public void addClient(GeneralRequest request, StreamObserver<GeneralResponse> responseObserver) {
    super.addClient(request, responseObserver);
  }

  @Override
  public void getTableHeader(GeneralRequest request, StreamObserver<HeaderProto> responseObserver) {
    HeaderProto headerProto = getVirtualTableHeader(request.getValue()).toProto();
    LOG.info("Get header of table {}", request.getValue());
    responseObserver.onNext(headerProto);
    responseObserver.onCompleted();
  }

  public List<TableInfo> getAllVirtualTable() {
    virtualLock.readLock().lock();
    List<TableInfo> infos = virtualTableInfoMap.values().stream().map(
      vinfo -> vinfo.getFakeTableInfo()
    ).collect(Collectors.toList());
    virtualLock.readLock().unlock();
    return infos;
  }

  @Override
  public void getAllLocalTable(GeneralRequest request, StreamObserver<LocalTableListProto> responseObserver) {
    LocalTableListProto.Builder builder = LocalTableListProto.newBuilder();
    getAllVirtualTable().forEach(info -> builder.addTable(info.toProto()));
    responseObserver.onNext(builder.build());
    LOG.info("Get {} local table infos", builder.getTableCount());
    responseObserver.onCompleted();
  }

  final protected boolean registerTable2Zk(String schema, String globalName, String localName) {
    if (zkClient == null) {
      LOG.warn("DBZkClient is not initialized, fail to register {} to {}/{}", localName, schema, globalName);
      return true;
    }
    return zkClient.registerTable(schema, globalName, endpoint, localName);
  }

  protected Header getVirtualTableHeader(String virtualTableName) {
    VirtualTableInfo info = virtualTableInfoMap.get(virtualTableName);
    if (info == null) {
      return Header.newBuilder().build();
    } else {
      return info.getFakeTableInfo().getHeader();
    }
  }

  final public void addLocalTableInfo(TableInfo tableInfo) {
    localLock.writeLock().lock();
    LOG.info("Add Local Table {}", tableInfo);
    localTableInfoMap.put(tableInfo.getName(), tableInfo);
    localLock.writeLock().unlock();
  }

  final public TableInfo getLocalTableInfo(String tableName) {
    localLock.readLock().lock();
    TableInfo info = localTableInfoMap.get(tableName);
    localLock.readLock().unlock();
    return info;
  }

  final public List<TableInfo> getAllLocalTable() {
    List<TableInfo> infos = new ArrayList<>();
    localLock.readLock().lock();
    for (TableInfo info : localTableInfoMap.values()) {
      infos.add(info);
    }
    localLock.readLock().unlock();
    return infos;
  }

  public void clearVirtualTable() {
    virtualLock.writeLock().lock();
    virtualTableInfoMap.clear();
    virtualLock.writeLock().unlock();
  }

  public void dropVirtualTable(String tableName) {
    virtualLock.writeLock().lock();
    virtualTableInfoMap.remove(tableName);
    virtualLock.writeLock().unlock();
  }

  public VirtualTableInfo configVirtualTableInfo(String localTableName, String virtualTableName, List<Field> fields) {
    TableInfo info = getLocalTableInfo(localTableName);
    return new VirtualTableInfo(info, virtualTableName, fields);
  }

  public boolean addVirtualTable(AliasTableInfo info) {
    VirtualTableInfo v = configVirtualTableInfo(info.getLocalTableName(), info.getVirtualTableName(), info.getFields());
    return addVirtualTable(v);
  }

  public boolean addVirtualTable(VirtualTableInfo virtualTable) {
    virtualLock.writeLock().lock();
    if (virtualTableInfoMap.containsKey(virtualTable.getVirtualTableName())) {
      LOG.error("virtual table {} already exist", virtualTable.getVirtualTableName());
      virtualLock.writeLock().unlock();
      return false;
    }
    LOG.info("Add Virtual Table {}", virtualTable);
    virtualTableInfoMap.put(virtualTable.getVirtualTableName(), virtualTable);
    virtualLock.writeLock().unlock();
    return true;
  }

  public boolean changeCatalog(String catalog) {
    LOG.error("change catalog operation is not supported in your database");
    return false;
  };

  // must call this function in subclass's constructor
  public abstract void loadAllTableInfo();

  protected abstract TableInfo loadTableInfo(String tableName);

  protected abstract void oneDBQueryInternal(OneDBQuery query, DataSet dataSet) throws SQLException;
}
