package com.hufudb.onedb.mpc.utils;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolException;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.RpcProtocolExecutor;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;

/**
 * boardcast data to all receiver
 */
public class Boardcast extends RpcProtocolExecutor {

  public Boardcast(Rpc rpc) {
    super(rpc, ProtocolType.BOARDCAST);
  }

  List<byte[]> senderProcedure(long taskId, List<Integer> receivers, List<byte[]> inputData,
      long extraInfo) {
    for (Integer recId : receivers) {
      DataPacketHeader header =
          new DataPacketHeader(taskId, getProtocolTypeId(), 0, extraInfo, ownId, recId);
      rpc.send(DataPacket.fromByteArrayList(header, inputData));
    }
    return ImmutableList.of();
  }

  List<byte[]> receiverProcedure(long taskId, int senderId, long extraInfo) {
    DataPacketHeader expect =
        new DataPacketHeader(taskId, getProtocolTypeId(), 0, extraInfo, senderId, ownId);
    DataPacket packet = rpc.receive(expect);
    if (packet == null) {
      LOG.error("Fail to receive boardcasted message in {}", rpc.ownParty());
      throw new RuntimeException("Fail to receive boardcasted message");
    } else {
      return packet.getPayload();
    }
  }

  /**
   * @param parties [senderId, receiverIds...]
   * @param args[0] inputdata
   * @param args[1] extraInfo
   * @throws ProtocolException
   */
  @Override
  public Object run(long taskId, List<Integer> parties, Object... args) throws ProtocolException {
    int senderId = parties.get(0);
    long extraInfo = 0;
    if (args.length > 1) {
      extraInfo = ((Number) args[1]).longValue();
    }
    if (senderId == ownId) {
      return senderProcedure(taskId, parties.subList(1, parties.size()), (List<byte[]>) args[0], extraInfo);
    } else {
      return receiverProcedure(taskId, senderId, extraInfo);
    }
  }
}
