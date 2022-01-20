package com.hufudb.onedb.core.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import com.hufudb.onedb.rpc.OneDBService.GeneralRequest;
import com.hufudb.onedb.rpc.OneDBService.GeneralResponse;
import com.hufudb.onedb.rpc.ServiceGrpc;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.utils.EmptyIterator;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.HeaderProto;
import com.hufudb.onedb.rpc.OneDBCommon.LocalTableListProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;

/*
* client for a single DB
*/
public class DBClient {
  private static final Logger LOG = LoggerFactory.getLogger(DBClient.class);

  private final ServiceGrpc.ServiceBlockingStub blockingStub;

  private String endpoint;

  public DBClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().maxInboundMessageSize(1024 * 1024 * 80));
    this.endpoint = String.format("%s:%d", host, port);
  }

  public DBClient(String endpoint) {
    this(ManagedChannelBuilder.forTarget(endpoint).usePlaintext().maxInboundMessageSize(1024 * 1024 * 80));
    this.endpoint = endpoint;
  }

  public DBClient(ManagedChannelBuilder<?> channelBuilder) {
    this(channelBuilder.build());
  }

  public DBClient(Channel channel) {
    blockingStub = ServiceGrpc.newBlockingStub(channel);
  }

  public boolean addClient(String endpoint) {
    GeneralRequest request = GeneralRequest.newBuilder().setValue(endpoint).build();
    GeneralResponse response;
    try {
      response = blockingStub.addClient(request);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed: {}", e);
      return false;
    }
    if (response.getStatus() == 0) {
      return true;
    } else {
      LOG.warn("error in addClient: {}", response.getMsg());
    }
    return true;
  }

  public Iterator<DataSetProto> oneDBQuery(OneDBQueryProto query) {
    try {
      return blockingStub.oneDBQuery(query);
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
    return String.format("DBClient[%s]", endpoint);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DBClient)) {
      return false;
    }
    return endpoint.equals(((DBClient) obj).endpoint);
  }

  public Header getTableHeader(String tableName) {
    try {
      HeaderProto proto = blockingStub
              .getTableHeader(GeneralRequest.newBuilder().setValue(tableName).build());
      return Header.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getTableHeader: {}", e.getStatus());
      return Header.newBuilder().build();
    }
  }

  public List<TableInfo> getAllLocalTable() {
    try {
      LocalTableListProto proto = blockingStub
              .getAllLocalTable(GeneralRequest.newBuilder().build());
      return TableInfo.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getAllLocalTable: {}", e.getStatus());
      return new ArrayList<>();
    }
  }
}
