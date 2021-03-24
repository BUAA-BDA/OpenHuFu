package tk.onedb.client;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import tk.onedb.OneDBService.AddClientRequest;
import tk.onedb.OneDBService.GeneralResponse;
import tk.onedb.OneDBService.Query;
import tk.onedb.ServiceGrpc;
import tk.onedb.core.utils.EmptyIterator;
import tk.onedb.rpc.OneDBCommon.DataSetProto;

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
    AddClientRequest request = AddClientRequest.newBuilder().setEndpoint(endpoint).build();
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

  public Iterator<DataSetProto> oneDBQuery(Query query) {
    try {
      return blockingStub.oneDBQuery(query);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in onDBQuery: {}", e.getStatus());
    }
    return new EmptyIterator<DataSetProto>();
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
}
