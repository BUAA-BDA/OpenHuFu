package group.bda.federate.client;

import group.bda.federate.rpc.FederateService.CompareEncDistanceRequest;
import group.bda.federate.rpc.FederateService.ComparePolyRequest;
import group.bda.federate.rpc.FederateService.ComparePolyResponse;
import java.util.Iterator;

import group.bda.federate.rpc.FederateService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.data.Header;
import group.bda.federate.rpc.FederateCommon.DataSetProto;
import group.bda.federate.rpc.FederateCommon.HeaderProto;
import group.bda.federate.rpc.FederateGrpc;
import group.bda.federate.rpc.FederateService.AddClientRequest;
import group.bda.federate.rpc.FederateService.CacheID;
import group.bda.federate.rpc.FederateService.Code;
import group.bda.federate.rpc.FederateService.DPRangeCountResponse;
import group.bda.federate.rpc.FederateService.GeneralResponse;
import group.bda.federate.rpc.FederateService.GetTableHeaderRequest;
import group.bda.federate.rpc.FederateService.KnnRadiusQueryResponse;
import group.bda.federate.rpc.FederateService.PrivacyQuery;
import group.bda.federate.rpc.FederateService.QValue;
import group.bda.federate.rpc.FederateService.Query;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

public final class FederateDBClient {
  private static final Logger LOG = LogManager.getLogger(FederateDBClient.class);

  private final FederateGrpc.FederateBlockingStub blockingStub;
  private final FederateGrpc.FederateStub asyncStub;
  private String endpoint;

  public FederateDBClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().maxInboundMessageSize(Integer.MAX_VALUE));
    this.endpoint = String.format("%s:%d", host, port);
  }

  public FederateDBClient(String endpoint) {
    this(ManagedChannelBuilder.forTarget(endpoint).usePlaintext().maxInboundMessageSize(Integer.MAX_VALUE));
    this.endpoint = endpoint;
  }

  public FederateDBClient(ManagedChannelBuilder<?> channelBuilder) {
    this(channelBuilder.build());
  }

  public FederateDBClient(Channel channel) {
    blockingStub = FederateGrpc.newBlockingStub(channel);
    asyncStub = FederateGrpc.newStub(channel);
  }

  public String getEndpoint() {
    return endpoint;
  }

  public boolean addClient(String endpoint) {
    AddClientRequest request = AddClientRequest.newBuilder().setEndpoint(endpoint).build();
    GeneralResponse response;
    try {
      response = blockingStub.addClient(request);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in add client: {}", e.getStatus());
      return false;
    }
    if (response.getStatus().getCode() != Code.kOk) {
      LOG.error("add client {} failed", endpoint);
      return false;
    } else {
      LOG.debug("add client {} ok", endpoint);
      return true;
    }
  }

  public double knnRadiusQuery(Query query, String uuid) {
    FederateService.KnnRadiusQueryRequest request = FederateService.KnnRadiusQueryRequest.newBuilder().
            setQuery(query).setUuid(uuid).build();
    KnnRadiusQueryResponse response;
    try {
      response = blockingStub.knnRadiusQuery(request);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in knn radius query: {}", e.getStatus());
      return Double.MAX_VALUE;
    }
    return response.getRadius();
  }

  public int privacyCompare(FederateService.PrivacyCompareRequest request) {
    FederateService.PrivacyCompareResponse response;
    try {
      response = blockingStub.privacyCompare(request);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in privacy compare: {}", e.getStatus());
      return 0;
    }
    int sum = 0;
    for (int s : response.getSharesList()) {
      sum += s;
    }
    return sum;
  }

  public int getMulValue(FederateService.GetMulValueRequest request) {
    try {
      return blockingStub.getMulValue(request).getVal();
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in get mul: {}", e.getStatus());
      return 0;
    }
  }

  public boolean privacyCount(FederateService.PrivacyCountRequest request) {
    FederateService.PrivacyCountResponse response;
    try {
      response = blockingStub.privacyCount(request);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in privacy count: {}", e.getStatus());
      return false;
    }
    return true;
  }

  public DPRangeCountResponse getDPRangeCountResult(FederateService.PrivacyCountRequest request) {
    DPRangeCountResponse response;
    try {
      response = blockingStub.dPRangeCount(request);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in dp range count: {}", e.getStatus());
      return null;
    }
    return response;
  }

  public double getSum(String uuid) {
    CacheID request = CacheID.newBuilder().setUuid(uuid).build();
    FederateService.PrivacyCountResponse response;
    try {
      response = blockingStub.getSum(request);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in get sum: {}", e.getStatus());
      return 0;
    }
    return response.getSum();
  }

  public boolean sendQValue(double qxi, String uuid) {
    QValue q = QValue.newBuilder().setQ(qxi).setUuid(uuid).build();
    GeneralResponse response;
    try {
      response = blockingStub.sendQValue(q);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in send Q value: {}", e.getStatus());
      return false;
    }
    // System.out.printf("send q [%d] to endpoint [%s]\n", qxi, endpoint);
    return response.getStatus().getCode() == Code.kOk;
  }

  public Header getTableHeader(String tableName) {
    try {
      HeaderProto proto = blockingStub
              .getTableHeader(GetTableHeaderRequest.newBuilder().setTableName(tableName).build());
      return Header.fromProto(proto);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in getTableHeader: {}", e.getStatus());
      return Header.newBuilder(0).build();
    }
  }

  public void clearCache(String uuid) {
    try {
      blockingStub.clearCache(CacheID.newBuilder().setUuid(uuid).build());
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in clear cache: {}", e.getStatus());
    }
  }

  public Iterator<DataSetProto> fedSpatialQuery(Query query) {
    try {
      return blockingStub.fedSpatialQuery(query);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in FedSpatialQuery: {}", e.getStatus());
      return null;
    }
  }

  public Iterator<DataSetProto> fedSpatialPrivacyQuery(PrivacyQuery query) {
    try {
      return blockingStub.fedSpatialPrivacyQuery(query);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in FedSpatialPrivacyQuery: {}", e.getStatus());
      return null;
    }
  }

  public ComparePolyRequest twoPartyDistanceCompare(CompareEncDistanceRequest request) {
    try {
      return blockingStub.twoPartyDistanceCompare(request);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in FedSpatialPrivacyQuery: {}", e.getStatus());
      return null;
    }
  }

  public GeneralResponse twoPartyDistanceCompareResult(ComparePolyResponse response) {
    try {
      return blockingStub.twoPartyDistanceCompareResult(response);
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in FedSpatialPrivacyQuery: {}", e.getStatus());
      return null;
    }
  }

  public Iterator<DataSetProto> getRandomDataSet(String uuid) {
    try {
      return blockingStub.getRandomDataSet(FederateService.CacheID.newBuilder()
              .setUuid(uuid).build());
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in sendDataSet: {}", e.getStatus());
      return new Iterator<DataSetProto>() {
        public boolean hasNext() {
          return false;
        }

        public DataSetProto next() {
          return null;
        }
      };
    }
  }

  @Override
  public String toString() {
    return String.format("DBClient[%s]", endpoint);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FederateDBClient)) {
      return false;
    }
    return endpoint.equals(((FederateDBClient) obj).endpoint);
  }
}
