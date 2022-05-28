package com.hufudb.onedb.mpc.utils;

import java.util.ArrayList;
import java.util.List;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.RpcProtocolExecutor;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;

/*
 * boardcast large data to all receiver, divide the data into small pieces and tranfer in stream
 * params:
 *   taskId, receiver parties, data to send (empty for receiver), senderId
 */
public class Stream extends RpcProtocolExecutor {
  static final long DEFAULT_MAX_SIZE = 4 * 1024 * 1023;

  private final long MAX_SIZE;

  public Stream(Rpc rpc) {
    super(rpc, ProtocolType.STREAM);
    MAX_SIZE = DEFAULT_MAX_SIZE;
  }

  @VisibleForTesting
  public Stream(Rpc rpc, long size) {
    super(rpc, ProtocolType.STREAM);
    MAX_SIZE = size;
  }

  void send(long taskId, int packetId, List<Integer> receivers, List<byte[]> input) {
    // todo: concurrent opt
    for (Integer receiverId :receivers) {
      DataPacketHeader header = new DataPacketHeader(taskId, getProtocolTypeId(), packetId, ownId, receiverId);
      rpc.send(DataPacket.fromByteArrayList(header, input));
    }
  }

  List<byte[]> senderProcedure(long taskId, List<Integer> receivers, List<byte[]> inputData) {
    long size = 0;
    int packetId = 0;
    int last = 0;
    for (int i = 0; i < inputData.size(); ++i) {
      byte[] ele = inputData.get(i);
      if (size + ele.length > MAX_SIZE) {
        send(taskId, packetId, receivers, inputData.subList(last, i));
        size = ele.length;
        last = i;
        packetId++;
      } else {
        size += ele.length;
      }
    }
    if (size != 0) {
      send(taskId, packetId, receivers, inputData.subList(last, inputData.size()));
    }
    // send an empty packet to stop the stream
    send(taskId, packetId + 1, receivers, ImmutableList.of());
    return ImmutableList.of();
  }

  List<byte[]> receiverProcedure(long taskId, int senderId) {
    List<byte[]> result = new ArrayList<>();
    int i = 0;
    while (true) {
      DataPacketHeader expect = new DataPacketHeader(taskId, getProtocolTypeId(), i, senderId, ownId);
      DataPacket unit = rpc.receive(expect);
      if (unit == null) {
        LOG.error("Stream transfor failed in {}", rpc.ownParty());
      } else if (unit.getPayload().size() == 0) {
        break;
      } else {
        result.addAll(unit.getPayload());
      }
      ++i;
    }
    return result;
  }

  // todo: add extraInfo to distinguish different request under the same task
  @Override
  public List<byte[]> run(long taskId, List<Integer> parties, List<byte[]> inputData,
      Object... args) {
    int senderId = (int) args[0];
    if (senderId == ownId) {
      return senderProcedure(taskId, parties, inputData);
    } else {
      return receiverProcedure(taskId, senderId);
    }
  }
}
