package com.hufudb.onedb.rpc.grpc.pipe;

import java.util.Map;
import com.hufudb.onedb.rpc.PipeGrpc;
import com.hufudb.onedb.rpc.OneDBPipe.DataPacketProto;
import com.hufudb.onedb.rpc.OneDBPipe.ResponseProto;
import com.hufudb.onedb.rpc.grpc.queue.ConcurrentBuffer;
import com.hufudb.onedb.rpc.utils.DataPacket;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeService extends PipeGrpc.PipeImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(PipeService.class);
  private static final ResponseProto OK = ResponseProto.newBuilder().build();

  private final Map<Integer, ConcurrentBuffer> buffers;

  /*
   * partyId -> concurrentBuffer
   * each buffer only collects packets from the correspongding party
   */
  public PipeService(Map<Integer, ConcurrentBuffer> buffers) {
    this.buffers = buffers;
  }

  @Override
  public StreamObserver<DataPacketProto> send(StreamObserver<ResponseProto> responseObserver) {
    return new StreamObserver<DataPacketProto>() {
      @Override
      public void onNext(DataPacketProto value) {
        int senderId = value.getHeaderProto().getSenderId();
        LOG.info("Get packet from sender [{}]", senderId);
        boolean hasErr = buffers.get(senderId).put(DataPacket.fromProto(value));
        if (hasErr) {
          responseObserver.onNext(ResponseProto.newBuilder().setStatus(1).setMsg(String.format("Buffer of party[%d] is full", senderId)).build());
        } else {
          responseObserver.onNext(OK);
        }
      }

      @Override
      public void onError(Throwable t) {
        LOG.warn("Error: {}", t.getMessage());
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
        LOG.info("Stop pipe service");
      }
    };
  }
}
