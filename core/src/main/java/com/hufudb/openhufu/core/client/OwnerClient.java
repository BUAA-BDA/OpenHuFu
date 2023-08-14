package com.hufudb.openhufu.core.client;

import com.hufudb.openhufu.core.utils.EmptyIterator;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.schema.TableSchema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.DataSetIterator;
import com.hufudb.openhufu.data.storage.ProtoDataSet;
import com.hufudb.openhufu.proto.OpenHuFuService.SaveRequest;
import com.hufudb.openhufu.rpc.grpc.OpenHuFuOwnerInfo;
import com.hufudb.openhufu.rpc.Party;
import com.hufudb.openhufu.proto.ServiceGrpc;
import com.hufudb.openhufu.proto.OpenHuFuData.DataSetProto;
import com.hufudb.openhufu.proto.OpenHuFuData.SchemaProto;
import com.hufudb.openhufu.proto.OpenHuFuData.TableSchemaListProto;
import com.hufudb.openhufu.proto.OpenHuFuPlan.QueryPlanProto;
import com.hufudb.openhufu.proto.OpenHuFuService.GeneralRequest;
import com.hufudb.openhufu.proto.OpenHuFuService.GeneralResponse;
import com.hufudb.openhufu.proto.OpenHuFuService.OwnerInfo;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * client for a single DB
 */
public class OwnerClient {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerClient.class);

  private final ServiceGrpc.ServiceBlockingStub blockingStub;

  private final String endpoint;
  private final Party party;

  public OwnerClient(String endpoint) {
    this(ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build(), endpoint);
    LOG.info("Connect to {} in plaintext", endpoint);
  }

  public OwnerClient(String endpoint, ChannelCredentials creds) {
    this(Grpc.newChannelBuilder(endpoint, creds).build(), endpoint);
    LOG.info("Connect to {} with TLS", endpoint);
  }

  public OwnerClient(Channel channel, String endpoint) throws StatusRuntimeException {
    this.blockingStub = ServiceGrpc.newBlockingStub(channel);
    this.endpoint = endpoint;
    this.party = getOwnerInfo();
  }

  public Party getParty() {
    return party;
  }

  public Party getOwnerInfo() {
    try {
      OwnerInfo proto = blockingStub.getOwnerInfo(GeneralRequest.newBuilder().build());
      return OpenHuFuOwnerInfo.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getOwnerInfo: {}", e);
      throw new RuntimeException("Fail to connect to owner " + endpoint);
    }
  }

  public boolean addOwner(Party party) {
    OwnerInfo request =
        OwnerInfo.newBuilder().setId(party.getPartyId()).setEndpoint(party.getPartyName()).build();
    GeneralResponse response;
    try {
      response = blockingStub.addOwner(request);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in addOwner: {}", e.getMessage());
      return false;
    }
    if (response.getStatus() == 0) {
      return true;
    } else {
      LOG.warn("error in addOwner: {}", response.getMsg());
    }
    return true;
  }

  public Iterator<DataSetProto> query(QueryPlanProto query) {
    try {
      return blockingStub.query(query);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in onDBQuery: {}", e.getStatus());
    }
    return new EmptyIterator<DataSetProto>();
  }

  public void saveResult(String tableName, DataSet result) {
    ProtoDataSet.Builder builder = ProtoDataSet.newBuilder(result.getSchema());
    DataSetIterator it = result.getIterator();
    while (it.next()) {
      builder.addRow(it);
    }
    SaveRequest saveRequest = SaveRequest.newBuilder().setTableName(tableName)
            .setData(builder.buildProto()).build();
    try {
      blockingStub.saveResult(saveRequest);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in onDBQuery: {}", e.getStatus());
    }
  }

  public String getEndpoint() {
    return endpoint;
  }

  @Override
  public String toString() {
    return String.format("OwnerClient[%s]", endpoint);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof OwnerClient)) {
      return false;
    }
    return endpoint.equals(((OwnerClient) obj).endpoint);
  }

  public Schema getTableSchema(String tableName) {
    try {
      SchemaProto proto =
          blockingStub.getTableSchema(GeneralRequest.newBuilder().setValue(tableName).build());
      return Schema.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getTableSchema: {}", e.getStatus());
      return Schema.newBuilder().build();
    }
  }

  public List<TableSchema> getAllLocalTable() {
    try {
      TableSchemaListProto proto =
          blockingStub.getAllTableSchema(GeneralRequest.newBuilder().build());
      return TableSchema.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getAllLocalTable: {}", e.getStatus());
      return new ArrayList<>();
    }
  }
}
