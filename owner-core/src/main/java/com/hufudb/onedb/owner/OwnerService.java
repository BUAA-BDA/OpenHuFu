package com.hufudb.onedb.owner;

import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.Field;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.PublishedTableInfo;
import com.hufudb.onedb.core.data.StreamObserverDataSet;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import com.hufudb.onedb.core.sql.rel.OneDBQueryContext;
import com.hufudb.onedb.core.zk.DBZkClient;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.HeaderProto;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableListProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;
import com.hufudb.onedb.rpc.OneDBService.GeneralRequest;
import com.hufudb.onedb.rpc.OneDBService.GeneralResponse;
import com.hufudb.onedb.rpc.ServiceGrpc;
import io.grpc.stub.StreamObserver;
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

@Service
public abstract class OwnerService extends ServiceGrpc.ServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerService.class);
  protected final Map<String, OwnerClient> dbClientMap; // endpoint -> rpc_client
  protected final String endpoint;
  private final Map<String, TableInfo> localTableInfoMap; // localName -> localTableInfo
  private final ReadWriteLock localLock;
  private final Map<String, PublishedTableInfo> publishedTableInfoMap; // publishedTableName ->
                                                                       // publishedTableInfo
  private final ReadWriteLock publishedLock;
  // private final ExecutorService executorService;
  private final DBZkClient zkClient;

  public OwnerService(String zkServers, String zkRootPath, String endpoint, String digest) {
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
    Header header = OneDBQueryContext.getOutputHeader(request);
    StreamObserverDataSet obDataSet = new StreamObserverDataSet(responseObserver, header);
    try {
      oneDBQueryInternal(request, obDataSet);
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
    Header fakeHeader = getPublishedTableHeader(request.getValue());
    HeaderProto headerProto = fakeHeader.toProto();
    LOG.info("Get header of table {} {}", request.getValue(), fakeHeader);
    responseObserver.onNext(headerProto);
    responseObserver.onCompleted();
  }

  public List<PublishedTableInfo> getAllPublishedTable() {
    publishedLock.readLock().lock();
    List<PublishedTableInfo> infos =
        publishedTableInfoMap.values().stream().map(vinfo -> vinfo).collect(Collectors.toList());
    publishedLock.readLock().unlock();
    return infos;
  }

  // todo: rename the funciton and rpc
  @Override
  public void getAllLocalTable(GeneralRequest request,
      StreamObserver<LocalTableListProto> responseObserver) {
    LocalTableListProto.Builder builder = LocalTableListProto.newBuilder();
    getAllPublishedTable().forEach(info -> builder.addTable(info.getFakeTableInfo().toProto()));
    responseObserver.onNext(builder.build());
    LOG.info("Get {} local table infos", builder.getTableCount());
    responseObserver.onCompleted();
  }

  protected final boolean registerTable2Zk(String schema, String globalName, String localName) {
    if (zkClient == null) {
      LOG.warn("DBZkClient is not initialized, fail to register {} to {}/{}", localName, schema,
          globalName);
      return true;
    }
    return zkClient.registerTable(schema, globalName, endpoint, localName);
  }

  protected String getOriginTableName(String publishedTableName) {
    return publishedTableInfoMap.get(publishedTableName).getOriginTableName();
  }

  protected Header getPublishedTableHeader(String publishedTableName) {
    PublishedTableInfo info = publishedTableInfoMap.get(publishedTableName);
    if (info == null) {
      LOG.warn("Published table [{}] not found", publishedTableName);
      return Header.EMPTY;
    } else {
      return info.getFakeHeader();
    }
  }

  protected Header getVirtualHeader(String publishedTableName) {
    PublishedTableInfo info = publishedTableInfoMap.get(publishedTableName);
    if (info == null) {
      return Header.EMPTY;
    } else {
      return info.getVirtualHeader();
    }
  }

  public final void addLocalTableInfo(TableInfo tableInfo) {
    localLock.writeLock().lock();
    LOG.info("Add Local Table {}", tableInfo);
    localTableInfoMap.put(tableInfo.getName(), tableInfo);
    localLock.writeLock().unlock();
  }

  public final TableInfo getLocalTableInfo(String tableName) {
    localLock.readLock().lock();
    TableInfo info = localTableInfoMap.get(tableName);
    localLock.readLock().unlock();
    return info;
  }

  public final List<TableInfo> getAllFakeTable() {
    localLock.readLock().lock();
    List<TableInfo> results = publishedTableInfoMap.values().stream()
        .map(info -> info.getFakeTableInfo()).collect(Collectors.toList());
    localLock.readLock().unlock();
    return results;
  }

  public final List<TableInfo> getAllLocalTable() {
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

  public void initPublishedTable(List<POJOPublishedTableInfo> infos) {
    for (POJOPublishedTableInfo info : infos) {
      addPublishedTable(info);
    }
  }

  public PublishedTableInfo generatePublishedTableInfo(POJOPublishedTableInfo publishedTableInfo) {
    List<Field> pFields = new ArrayList<>();
    List<Integer> mappings = new ArrayList<>();
    TableInfo originInfo = localTableInfoMap.get(publishedTableInfo.getOriginTableName());
    List<Field> publishedFields = publishedTableInfo.getPublishedFields();
    List<Integer> originNames = publishedTableInfo.getOriginColumns();
    for (int i = 0; i < publishedFields.size(); ++i) {
      if (!publishedFields.get(i).getLevel().equals(Level.HIDDEN)) {
        pFields.add(publishedFields.get(i));
        mappings.add(originNames.get(i));
      }
    }
    return new PublishedTableInfo(originInfo, publishedTableInfo.getPublishedTableName(), pFields,
        mappings);
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
  }

  // must call this function in subclass's constructor
  public abstract void loadAllTableInfo();

  protected abstract TableInfo loadTableInfo(String tableName);

  protected abstract void oneDBQueryInternal(OneDBQueryProto query, DataSet dataSet)
      throws SQLException;
}
