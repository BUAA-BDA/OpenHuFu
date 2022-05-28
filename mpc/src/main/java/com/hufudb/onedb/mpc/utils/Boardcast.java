package com.hufudb.onedb.mpc.utils;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.RpcProtocolExecutor;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;

/*
 * boardcast data to all receiver
 * params:
 *   taskId, receiver parties, data to send (empty for receiver), senderId, extraInfo
 */
public class Boardcast extends RpcProtocolExecutor {

  public Boardcast(Rpc rpc) {
    super(rpc, ProtocolType.BOARDCAST);
  }

  List<byte[]> senderProcedure(long taskId, List<Integer> receivers, List<byte[]> inputData, long extraInfo) {
    for (Integer recId : receivers) {
      DataPacketHeader header = new DataPacketHeader(taskId, getProtocolTypeId(), 0, extraInfo, ownId, recId);
      rpc.send(DataPacket.fromByteArrayList(header, inputData));
    }
    return ImmutableList.of();
  }

  List<byte[]> receiverProcedure(long taskId, int senderId, long extraInfo) {
    DataPacketHeader expect = new DataPacketHeader(taskId, getProtocolTypeId(), 0, extraInfo, senderId, ownId);
    DataPacket packet = rpc.receive(expect);
    if (packet == null) {
      LOG.error("Fail to receive boardcasted message in {}", rpc.ownParty());
      throw new RuntimeException("Fail to receive boardcasted message");
    } else {
      return packet.getPayload();
    }
  }

  @Override
  public List<byte[]> run(long taskId, List<Integer> parties, List<byte[]> inputData,
      Object... args) {
    int senderId = (int) args[0];
    long extraInfo = 0;
    if (args.length > 1) {
      extraInfo = ((Number) args[1]).longValue();
    }
    if (senderId == ownId) {
      return senderProcedure(taskId, parties, inputData, extraInfo);
    } else {
      return receiverProcedure(taskId, senderId, extraInfo);
    }
  }
}
