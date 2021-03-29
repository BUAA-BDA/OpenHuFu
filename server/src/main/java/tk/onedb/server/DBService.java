package tk.onedb.server;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import tk.onedb.OneDBService.GeneralRequest;
import tk.onedb.OneDBService.GeneralResponse;
import tk.onedb.ServiceGrpc;
import tk.onedb.core.client.DBClient;
import tk.onedb.core.config.OneDBConfig;
import tk.onedb.core.data.DataSet;
import tk.onedb.core.data.Header;
import tk.onedb.core.data.StreamObserverDataSet;
import tk.onedb.rpc.OneDBCommon.DataSetProto;
import tk.onedb.rpc.OneDBCommon.HeaderProto;
import tk.onedb.rpc.OneDBCommon.OneDBQueryProto;
import tk.onedb.server.data.TableInfo;

public abstract class DBService extends ServiceGrpc.ServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(DBService.class);
  protected final Map<String, TableInfo> tableInfoMap;
  protected final Map<String, DBClient> dbClientMap;
  protected final Lock clientLock;
  private final ExecutorService executorService;

  protected DBService() {
    this.tableInfoMap = new HashMap<>();
    this.dbClientMap = new HashMap<>();
    this.clientLock = new ReentrantLock();
    this.executorService = Executors.newFixedThreadPool(OneDBConfig.SERVER_THREAD_NUM);
  }

  @Override
  public void oneDBQuery(OneDBQueryProto request, StreamObserver<DataSetProto> responseObserver) {
    StreamObserverDataSet obDataSet = new StreamObserverDataSet(responseObserver, Header.fromProto(request.getHeader()));
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
    HeaderProto headerProto = getTableHeader(request.getValue()).toProto();
    responseObserver.onNext(headerProto);
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

  protected abstract void oneDBQueryInternal(OneDBQueryProto query, DataSet dataSet) throws SQLException;
}
