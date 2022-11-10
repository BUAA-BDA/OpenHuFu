package com.hufudb.onedb.rpc.grpc.pipe;

import com.hufudb.onedb.proto.PipeGrpc;
import com.hufudb.onedb.proto.DataPacket.DataPacketProto;
import com.hufudb.onedb.proto.DataPacket.ResponseProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

public class PipeClient {
  private static final Logger LOG = LoggerFactory.getLogger(PipeClient.class);

  private PipeGrpc.PipeBlockingStub stub;
  private final Channel channel;
  private final String endpoint;

  public PipeClient(Channel channel) {
    this.channel = channel;
    this.endpoint = channel.toString();
  }

  public PipeClient(String endpoint) {
    this(endpoint, null);
  }

  public PipeClient(String endpoint, ChannelCredentials certRoot) {
    if (certRoot == null) {
      channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
      LOG.info("Channel to {} in plaintext", endpoint);
    } else {
      channel = Grpc.newChannelBuilder(endpoint, certRoot).build();
      LOG.info("Channel to {} with TLS", endpoint);
    }
    this.endpoint = endpoint;
  }

  public void connect() {
    if (stub != null) {
      LOG.info("Connection to {} has already established", endpoint);
      return;
    }
    stub = PipeGrpc.newBlockingStub(channel);
    LOG.info("Connect to {}", endpoint);
  }

  public void send(DataPacketProto packet) {
    if (stub == null) {
      LOG.warn("No connection to {}", endpoint);
      return;
    }
    LOG.debug("Send packet to {}", endpoint);
    try {
      ResponseProto resp = stub.send(packet);
      if (resp.getStatus() != 0) {
        LOG.error("Error when send message in pipe: {}", resp.getMsg());
      }
    } catch (StatusRuntimeException e) {
      LOG.error("RPC failed in send packet: {}", e.getMessage());
    }
  }

  public void close() {
    if (stub == null) {
      LOG.warn("Connection to {} has already close", endpoint);
      return;
    }
    stub = null;
  }
}
