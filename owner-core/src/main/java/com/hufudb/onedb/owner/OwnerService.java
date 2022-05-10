package com.hufudb.onedb.owner;

import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.PublishedTableInfo;
import com.hufudb.onedb.core.data.StreamObserverDataSet;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.config.OwnerConfig;
import com.hufudb.onedb.owner.implementor.OwnerSideImplementor;
import com.hufudb.onedb.owner.schema.SchemaManager;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.HeaderProto;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableListProto;
import com.hufudb.onedb.rpc.OneDBCommon.OwnerInfoProto;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;
import com.hufudb.onedb.rpc.OneDBService.GeneralRequest;
import com.hufudb.onedb.rpc.OneDBService.GeneralResponse;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import com.hufudb.onedb.rpc.grpc.concurrent.ConcurrentBuffer;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.ServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OwnerService extends ServiceGrpc.ServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerService.class);
  protected final String endpoint;
  protected final ExecutorService threadPool;
  protected final OneDBRpc ownerSideRpc;
  protected final ConcurrentBuffer<Long, DataSet> resultBuffer; // taskId -> bufferDataSet
  protected final OwnerSideImplementor implementor;
  protected final Adapter adapter;
  protected final SchemaManager schemaManager;

  public OwnerService(OwnerConfig config) {
    this.threadPool = config.threadPool;
    this.endpoint = String.format("%s:%d", config.hostname, config.port);
    this.ownerSideRpc = config.acrossOwnerRpc;
    this.resultBuffer = new ConcurrentBuffer<Long, DataSet>();
    this.adapter = config.adapter;
    this.implementor = new OwnerSideImplementor(ownerSideRpc, adapter, threadPool);
    this.schemaManager = this.adapter.getSchemaManager();
    initPublishedTable(config.tables);
  }

  @Override
  public void query(QueryContextProto request, StreamObserver<DataSetProto> responseObserver) {
    OneDBContext context = OneDBContext.fromProto(request);
    Header header = OneDBContext.getOutputHeader(context);
    StreamObserverDataSet obDataSet = new StreamObserverDataSet(responseObserver, header);
    try {
      QueryableDataSet result = implementor.implement(context);
      obDataSet.addRows(result.getRows());
    } catch (Exception e) {
      LOG.error("Error when query table [{}]", request.getTableName());
      e.printStackTrace();
    }
    obDataSet.close();
  }

  @Override
  public void getOwnerInfo(GeneralRequest request,
      StreamObserver<OwnerInfoProto> responseObserver) {
    Party party = ownerSideRpc.ownParty();
    LOG.info("Get owner info {}", party);
    responseObserver.onNext(
        OwnerInfoProto.newBuilder().setId(party.getPartyId()).setEndpoint(endpoint).build());
    responseObserver.onCompleted();
  }

  @Override
  public void addOwner(OwnerInfoProto request, StreamObserver<GeneralResponse> responseObserver) {
    LOG.info("Connect to owner {}", OneDBOwnerInfo.fromProto(request));
    boolean ok = ownerSideRpc.addParty(OneDBOwnerInfo.fromProto(request));
    ownerSideRpc.connect();
    responseObserver.onNext(GeneralResponse.newBuilder().setStatus(ok ? 0 : 1)
        .setMsg(ok ? "" : "Fail to add owner").build());
    responseObserver.onCompleted();
  }

  @Override
  public void getTableHeader(GeneralRequest request, StreamObserver<HeaderProto> responseObserver) {
    Header fakeHeader = getPublishedTableHeader(request.getValue());
    HeaderProto headerProto = fakeHeader.toProto();
    LOG.info("Get header of table {} {}", request.getValue(), fakeHeader);
    responseObserver.onNext(headerProto);
    responseObserver.onCompleted();
  }

  public ExecutorService getThreadPool() {
    return threadPool;
  }

  public OneDBRpc getOwnerSideRpc() {
    return ownerSideRpc;
  }

  public List<PublishedTableInfo> getAllPublishedTable() {
    return schemaManager.getAllPublishedTable();
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

  protected String getLocalTableName(String publishedTableName) {
    return schemaManager.getLocalTableName(publishedTableName);
  }

  protected Header getPublishedTableHeader(String publishedTableName) {
    return schemaManager.getPublishedTableHeader(publishedTableName);
  }

  public TableInfo getLocalTableInfo(String tableName) {
    return schemaManager.getLocalTable(tableName);
  }

  public List<TableInfo> getAllLocalTable() {
    return schemaManager.getAllLocalTable();
  }

  public void clearPublishedTable() {
    schemaManager.clearPublishedTable();
  }

  public void dropPublishedTable(String tableName) {
    schemaManager.dropPublishedTable(tableName);
  }

  public void initPublishedTable(List<POJOPublishedTableInfo> infos) {
    if (infos == null) {
      return;
    }
    for (POJOPublishedTableInfo info : infos) {
      schemaManager.addPublishedTable(info);
    }
  }

  public boolean changeCatalog(String catalog) {
    LOG.error("change catalog operation is not supported in your database");
    return false;
  }

  protected void shutdown() {
    adapter.shutdown();
  };
}
