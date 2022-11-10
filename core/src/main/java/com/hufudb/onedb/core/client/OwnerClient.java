package com.hufudb.onedb.core.client;

import com.hufudb.onedb.core.utils.EmptyIterator;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.proto.ServiceGrpc;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;
import com.hufudb.onedb.proto.OneDBData.SchemaProto;
import com.hufudb.onedb.proto.OneDBData.TableSchemaListProto;
import com.hufudb.onedb.proto.OneDBPlan.QueryPlanProto;
import com.hufudb.onedb.proto.OneDBService.GeneralRequest;
import com.hufudb.onedb.proto.OneDBService.GeneralResponse;
import com.hufudb.onedb.proto.OneDBService.OwnerInfo;
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
 * it will build connection with the remote Owner
 * by assigned endpoint
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

  /**
   * fetch owner info from the remote Owner by RPC
   * @return
   */
  public Party getOwnerInfo() {
    try {
      OwnerInfo proto = blockingStub.getOwnerInfo(GeneralRequest.newBuilder().build());
      return OneDBOwnerInfo.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getOwnerInfo: {}", e.getStatus());
      throw new RuntimeException("Fail to connect to owner " + endpoint);
    }
  }

  /**
   * tell the remote Owner that a new owner participates
   * @param party
   * @return
   */
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

  /**
   * fetch table schema from remote Owner
   * @param tableName
   * @return
   */
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
