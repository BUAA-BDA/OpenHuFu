package com.hufudb.onedb.server;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import com.hufudb.onedb.rpc.OneDBService.GeneralRequest;
import com.hufudb.onedb.rpc.OneDBService.GeneralResponse;
import com.hufudb.onedb.rpc.ServiceGrpc;
import com.hufudb.onedb.core.client.DBClient;
import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.StreamObserverDataSet;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.sql.expression.OneDBQuery;
import com.hufudb.onedb.core.zk.DBZkClient;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.HeaderProto;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableListProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;
import com.hufudb.onedb.server.data.ServerConfig;
import com.hufudb.onedb.server.data.ServerConfig.Mapping;

public abstract class DBService extends ServiceGrpc.ServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(DBService.class);
  private final Map<String, TableInfo> tableInfoMap;
  protected final Map<String, DBClient> dbClientMap;
  protected final Lock clientLock;
  // private final ExecutorService executorService;
  private final DBZkClient zkClient;
  protected final String endpoint;

  protected DBService(String zkServers, String zkRootPath, String endpoint, String digest) {
    this.tableInfoMap = new HashMap<>();
    this.dbClientMap = new HashMap<>();
    this.clientLock = new ReentrantLock();
    // this.executorService = Executors.newFixedThreadPool(OneDBConfig.SERVER_THREAD_NUM);
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
    HeaderProto headerProto = getTableHeader(request.getValue()).toProto();
    LOG.info("Get header of table {}", request.getValue());
    responseObserver.onNext(headerProto);
    responseObserver.onCompleted();
  }

  @Override
  public void getAllLocalTable(GeneralRequest request, StreamObserver<LocalTableListProto> responseObserver) {
    LocalTableListProto.Builder builder = LocalTableListProto.newBuilder();
    for (TableInfo info : tableInfoMap.values()) {
      builder.addTable(info.toProto());
    }
    responseObserver.onNext(builder.build());
    LOG.info("Get {} local table infos", builder.getTableCount());
    responseObserver.onCompleted();
  }

  protected Header getTableHeader(String name) {
    TableInfo info = tableInfoMap.get(name);
    if (info == null) {
      return Header.newBuilder().build();
    } else {
      return info.getHeader();
    }
  }

  final protected void addTable(ServerConfig.Table table) throws SQLException {
    TableInfo tableInfo = loadTableInfo(table);
    LOG.info("add {}", tableInfo.toString());
    addTableInfo(tableInfo);
    for (Mapping m : table.mappings) {
      registerTable(m.schema, m.name, tableInfo.getName());
    }
  }

  final protected void addTableInfo(TableInfo tableInfo) {
    tableInfoMap.put(tableInfo.getName(), tableInfo);
  }

  final protected TableInfo getTableInfo(String tableName) {
    return tableInfoMap.get(tableName);
  }

  final protected boolean registerTable(String schema, String globalName, String localName) {
    if (zkClient == null) {
      LOG.warn("DBZkClient is not initialized, fail to register {} to {}/{}", localName, schema, globalName);
      return true;
    }
    return zkClient.registerTable2Schema(schema, globalName, endpoint, localName);
  }

  protected abstract TableInfo loadTableInfo(ServerConfig.Table table);

  protected abstract void oneDBQueryInternal(OneDBQuery query, DataSet dataSet) throws SQLException;
}
