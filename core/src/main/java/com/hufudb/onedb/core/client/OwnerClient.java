package com.hufudb.onedb.core.client;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.utils.EmptyIterator;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.HeaderProto;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableListProto;
import com.hufudb.onedb.rpc.OneDBCommon.LeafQueryProto;
import com.hufudb.onedb.rpc.OneDBCommon.OwnerInfoProto;
import com.hufudb.onedb.rpc.OneDBService.GeneralRequest;
import com.hufudb.onedb.rpc.OneDBService.GeneralResponse;
import com.hufudb.onedb.rpc.grpc.OneDBOwnerInfo;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.ServiceGrpc;
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

/*
 * client for a single DB
 */
public class OwnerClient {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerClient.class);

  private final ServiceGrpc.ServiceBlockingStub blockingStub;

  private final String endpoint;
  private final Party party;

  public OwnerClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build(), String.format("%s:%d", host, port));
    LOG.info("Connect to {}", endpoint);
  }

  public OwnerClient(String endpoint) {
    this(ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build(), endpoint);
    LOG.info("Connect to {}", endpoint);
  }

  public OwnerClient(String endpoint, ChannelCredentials creds) {
    this(Grpc.newChannelBuilder(endpoint, creds).build(), endpoint);
    LOG.info("Connect to {} with TLS", endpoint);
  }

  public OwnerClient(Channel channel, String endpoint) {
    this.blockingStub = ServiceGrpc.newBlockingStub(channel);
    this.endpoint = endpoint;
    this.party = getOwnerInfo();
  }

  public Party getParty() {
    return party;
  }

  public Party getOwnerInfo() {
    try {
      OwnerInfoProto proto =
          blockingStub.getOwnerInfo(GeneralRequest.newBuilder().build());
      return OneDBOwnerInfo.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getOwnerInfo: {}", e.getStatus());
      return null;
    }
  }

  public boolean addOwner(Party party) {
    OwnerInfoProto request = OwnerInfoProto.newBuilder().setId(party.getPartyId()).setEndpoint(party.getPartyName()).build();
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

  public Iterator<DataSetProto> leafQuery(LeafQueryProto query) {
    try {
      return blockingStub.leafQuery(query);
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

  public Header getTableHeader(String tableName) {
    try {
      HeaderProto proto =
          blockingStub.getTableHeader(GeneralRequest.newBuilder().setValue(tableName).build());
      return Header.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getTableHeader: {}", e.getStatus());
      return Header.newBuilder().build();
    }
  }

  public List<TableInfo> getAllLocalTable() {
    try {
      LocalTableListProto proto =
          blockingStub.getAllLocalTable(GeneralRequest.newBuilder().build());
      return TableInfo.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getAllLocalTable: {}", e.getStatus());
      return new ArrayList<>();
    }
  }
}
