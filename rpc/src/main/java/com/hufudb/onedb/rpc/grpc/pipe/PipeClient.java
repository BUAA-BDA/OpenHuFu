package com.hufudb.onedb.rpc.grpc.pipe;

import java.io.File;
import com.hufudb.onedb.rpc.PipeGrpc;
import com.hufudb.onedb.rpc.OneDBPipe.DataPacketProto;
import com.hufudb.onedb.rpc.OneDBPipe.ResponseProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class PipeClient {
  private static final Logger LOG = LoggerFactory.getLogger(PipeClient.class);

  private final PipeGrpc.PipeStub stub;
  private final String endpoint;
  private StreamObserver<DataPacketProto> reqObserver;

  public PipeClient(Channel channel) {
    this.stub = PipeGrpc.newStub(channel);
    this.endpoint = channel.toString();
  }

  public PipeClient(String endpoint) {
    this.stub = PipeGrpc.newStub(ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build());
    this.endpoint = endpoint;
    LOG.info("Pipe connect to {} in plaintext", endpoint);
  }

  public PipeClient(String endpoint, ChannelCredentials certRoot) {
    if (certRoot == null) {
      this.stub = PipeGrpc.newStub(ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build());
      LOG.info("Pipe connect to {} in plaintext", endpoint);
    } else {
      this.stub = PipeGrpc.newStub(Grpc.newChannelBuilder(endpoint, certRoot).build());
      LOG.info("Pipe connect to {} with TLS", endpoint);
    }
    this.endpoint = endpoint;
  }

  public void connect() {
    if (reqObserver != null) {
      LOG.info("Connection has established");
      return;
    }
    this.reqObserver = stub.send(
      new StreamObserver<ResponseProto>() {
        @Override
        public void onNext(ResponseProto resp) {
          if (resp.getStatus() != 0) {
            LOG.warn("Exception status:{}", resp.getMsg());
          }
        }

        @Override
        public void onError(Throwable t) {
          LOG.warn("Error in pipe: {}", t.getMessage());
          t.printStackTrace();
          PipeClient.this.reqObserver = null;
        }

        @Override
        public void onCompleted() {
          LOG.info("Close connection to {}", endpoint);
          PipeClient.this.reqObserver = null;
        }
      }
    );
  }

  public void send(DataPacketProto packet) {
    if (reqObserver == null) {
      LOG.warn("No connection to {}", endpoint);
      return;
    }
    LOG.debug("Send packet to {}", endpoint);
    reqObserver.onNext(packet);
  }

  public void close() {
    if (reqObserver == null) {
      LOG.warn("Connection to {} has already close", endpoint);
      return;
    }
    reqObserver.onCompleted();
    reqObserver = null;
  }
}
