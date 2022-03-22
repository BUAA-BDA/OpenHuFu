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
import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.Field;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.StreamObserverDataSet;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import com.hufudb.onedb.core.data.PublishedTableInfo;
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
  private final Map<String, PublishedTableInfo> publishedTableInfoMap; // publishedTableName -> publishedTableInfo
  private final ReadWriteLock publishedLock;
  // private final ExecutorService executorService;
  private final DBZkClient zkClient;
  protected final String endpoint;

  public DBService(String zkServers, String zkRootPath, String endpoint, String digest) {
    this.dbClientMap = new HashMap<>();
    this.localTableInfoMap = new HashMap<>();
    this.publishedTableInfoMap = new HashMap<>();
    // this.executorService =
    // Executors.newFixedThreadPool(OneDBConfig.SERVER_THREAD_NUM);
    this.localLock = new ReentrantReadWriteLock();
    this.publishedLock = new ReentrantReadWriteLock();
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
    HeaderProto headerProto = getPublishedTableHeader(request.getValue()).toProto();
    LOG.info("Get header of table {}", request.getValue());
    responseObserver.onNext(headerProto);
    responseObserver.onCompleted();
  }

  public List<PublishedTableInfo> getAllPublishedTable() {
    publishedLock.readLock().lock();
    List<PublishedTableInfo> infos = publishedTableInfoMap.values().stream().map(
        vinfo -> vinfo).collect(Collectors.toList());
    publishedLock.readLock().unlock();
    return infos;
  }

  // todo: rename the funciton and rpc
  @Override
  public void getAllLocalTable(GeneralRequest request, StreamObserver<LocalTableListProto> responseObserver) {
    LocalTableListProto.Builder builder = LocalTableListProto.newBuilder();
    getAllPublishedTable().forEach(info -> builder.addTable(info.getFakeTableInfo().toProto()));
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

  protected Header getPublishedTableHeader(String publishedTableName) {
    PublishedTableInfo info = publishedTableInfoMap.get(publishedTableName);
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

  public void clearPublishedTable() {
    publishedLock.writeLock().lock();
    publishedTableInfoMap.clear();
    publishedLock.writeLock().unlock();
  }

  public void dropPublishedTable(String tableName) {
    publishedLock.writeLock().lock();
    publishedTableInfoMap.remove(tableName);
    publishedLock.writeLock().unlock();
  }

  public void initPublishedTable(List<PublishedTableInfo> infos) {
    for (PublishedTableInfo info : infos) {
      addPublishedTable(info);
    }
  }

  public PublishedTableInfo generatePublishedTableInfo(POJOPublishedTableInfo publishedTableInfo) {
    List<Field> pFields = new ArrayList<>();
    List<Integer> mappings = new ArrayList<>();
    TableInfo originInfo = localTableInfoMap.get(publishedTableInfo.getOriginTableName());
    List<Field> publishedFields = publishedTableInfo.getPublishedFields();
    List<String> originNames = publishedTableInfo.getOriginNames();
    for (int i = 0; i < publishedFields.size(); ++i) {
      if (!publishedFields.get(i).getLevel().equals(Level.HIDDEN)) {
        pFields.add(publishedFields.get(i));
        mappings.add(originInfo.getColumnIndex(originNames.get(i)));
      }
    }
    return new PublishedTableInfo(originInfo, publishedTableInfo.getPublishedTableName(), pFields, mappings);
  }

  public boolean addPublishedTable(POJOPublishedTableInfo publishedTableInfo) {
    return addPublishedTable(generatePublishedTableInfo(publishedTableInfo));
  }

  public boolean addPublishedTable(PublishedTableInfo publishedTable) {
    publishedLock.writeLock().lock();
    if (publishedTableInfoMap.containsKey(publishedTable.getPublishedTableName())) {
      LOG.error("published table {} already exist", publishedTable.getPublishedTableName());
      publishedLock.writeLock().unlock();
      return false;
    }
    LOG.info("Add Published Table {}", publishedTable);
    publishedTableInfoMap.put(publishedTable.getPublishedTableName(), publishedTable);
    publishedLock.writeLock().unlock();
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
