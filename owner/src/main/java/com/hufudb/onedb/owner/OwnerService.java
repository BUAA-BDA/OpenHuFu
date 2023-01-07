package com.hufudb.onedb.owner;

import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.checker.Checker;
import com.hufudb.onedb.owner.config.OwnerConfig;
import com.hufudb.onedb.owner.implementor.OwnerSideImplementor;
import com.hufudb.onedb.owner.storage.StreamDataSet;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.data.schema.PublishedTableSchema;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.grpc.OneDBRpc;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.proto.ServiceGrpc;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;
import com.hufudb.onedb.proto.OneDBData.SchemaProto;
import com.hufudb.onedb.proto.OneDBData.TableSchemaListProto;
import com.hufudb.onedb.proto.OneDBPlan.QueryPlanProto;
import com.hufudb.onedb.proto.OneDBService.GeneralRequest;
import com.hufudb.onedb.proto.OneDBService.GeneralResponse;
import com.hufudb.onedb.proto.OneDBService.OwnerInfo;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
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
  protected final OwnerSideImplementor implementor;
  protected final Adapter adapter;
  protected final Map<ProtocolType, ProtocolExecutor> libraries;
  protected final SchemaManager schemaManager;

  public OwnerService(OwnerConfig config) {
    this.threadPool = config.threadPool;
    this.endpoint = String.format("%s:%d", config.hostname, config.port);
    this.ownerSideRpc = config.acrossOwnerRpc;
    this.adapter = config.adapter;
    this.implementor = new OwnerSideImplementor(ownerSideRpc, adapter, threadPool);
    this.schemaManager = this.adapter.getSchemaManager();
    this.libraries = config.librarys;
    initPublishedTable(config.tables);
    schemaManager.checkDesensitization();
  }

  @Override
  public void query(QueryPlanProto request, StreamObserver<DataSetProto> responseObserver) {
    Plan plan = Plan.fromProto(request);
    LOG.info("receives plan:\n{}", plan);
    Checker.checkSensitivity(plan, schemaManager);
    LOG.info("add sensitivity plan:\n{}", plan);
    if (!Checker.check(plan, schemaManager)) {
      LOG.warn("Check fail for plan {}", request.toString());
      responseObserver.onCompleted();
      return;
    }
    try {
      DataSet result = implementor.implement(plan);
      StreamDataSet output = new StreamDataSet(result, responseObserver);
      output.stream();
      output.close();
    } catch (Exception e) {
      LOG.error("Error in query");
      e.printStackTrace();
    }
  }

  @Override
  public void getOwnerInfo(GeneralRequest request,
      StreamObserver<OwnerInfo> responseObserver) {
    Party party = ownerSideRpc.ownParty();
    LOG.info("Get owner info {}", party);
    responseObserver.onNext(
        OwnerInfo.newBuilder().setId(party.getPartyId()).setEndpoint(endpoint).build());
    responseObserver.onCompleted();
  }

  @Override
  public void addOwner(OwnerInfo request, StreamObserver<GeneralResponse> responseObserver) {
    LOG.info("Connect to owner {}", OneDBOwnerInfo.fromProto(request));
    boolean ok = ownerSideRpc.addParty(OneDBOwnerInfo.fromProto(request));
    ownerSideRpc.connect();
    responseObserver.onNext(GeneralResponse.newBuilder().setStatus(ok ? 0 : 1)
        .setMsg(ok ? "" : "Fail to add owner").build());
    responseObserver.onCompleted();
  }

  @Override
  public void getTableSchema(GeneralRequest request, StreamObserver<SchemaProto> responseObserver) {
    Schema fakeSchema = getPublishedTableHeader(request.getValue());
    SchemaProto schemaProto = fakeSchema.toProto();
    LOG.info("Get schema of table {} {}", request.getValue(), fakeSchema);
    responseObserver.onNext(schemaProto);
    responseObserver.onCompleted();
  }

  public ExecutorService getThreadPool() {
    return threadPool;
  }

  public OneDBRpc getOwnerSideRpc() {
    return ownerSideRpc;
  }

  public List<PublishedTableSchema> getAllPublishedTable() {
    return schemaManager.getAllPublishedTable();
  }

  @Override
  public void getAllTableSchema(GeneralRequest request,
      StreamObserver<TableSchemaListProto> responseObserver) {
    TableSchemaListProto.Builder builder = TableSchemaListProto.newBuilder();
    getAllPublishedTable().forEach(info -> builder.addTable(info.getFakeTableSchema().toProto()));
    responseObserver.onNext(builder.build());
    LOG.info("Get {} local table schemas", builder.getTableCount());
    responseObserver.onCompleted();
  }

  protected String getLocalTableName(String publishedTableName) {
    return schemaManager.getActualTableName(publishedTableName);
  }

  protected Schema getPublishedTableHeader(String publishedTableName) {
    return schemaManager.getPublishedSchema(publishedTableName);
  }

  public TableSchema getLocalTableSchema(String tableName) {
    return schemaManager.getLocalTable(tableName);
  }

  public List<TableSchema> getAllLocalTable() {
    return schemaManager.getAllLocalTable();
  }

  public void clearPublishedTable() {
    schemaManager.clearPublishedTable();
  }

  public void dropPublishedTable(String tableName) {
    schemaManager.dropPublishedTable(tableName);
  }

  public void initPublishedTable(List<PojoPublishedTableSchema> schemas) {
    if (schemas == null) {
      return;
    }
    for (PojoPublishedTableSchema schema : schemas) {
      schemaManager.addPublishedTable(schema);
    }
  }

  public boolean addPublishedTable(PojoPublishedTableSchema schema) {
    return schemaManager.addPublishedTable(schema);
  }

  public boolean changeCatalog(String catalog) {
    LOG.error("change catalog operation is not supported in your database");
    return false;
  }

  protected void shutdown() {
    adapter.shutdown();
  }
}
