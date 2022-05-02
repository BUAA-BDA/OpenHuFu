package com.hufudb.onedb.mpc.psi;

import java.nio.ByteBuffer;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.codec.HashFunction;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import org.apache.commons.lang3.tuple.Pair;

/*-
 * Hash based PSI implementation
 *   Participants: S and R
 *   Init DataPacket
 *     S:
 *       Header: [ptoId: hpsi, stepId, 0, senderId: S, receiverId: R, extraInfo: hashFuncId]
 *       Payload: [byte array of keys]
 *     R:
 *       Header: [ptoId: hpsi, stepId, 0, senderId: S, receiverId: R, extraInfo: hashFuncId]
 *       Payload: [byte array of keys]
 *   Step1:
 *     S and R send number of elements of its set to each other, and choose the smaller one 
 *     as new sender NS (todo: change to secure comparison).
 *     S/R:
 *       Header: [ptoId: hpsi, stepId, 1, senderId: S/R, receiverId: R/S, extraInfo: hashFuncId]
 *       Payload: [byte arrary of set size]
 *   Step2:
 *     NS/NR hashes their local set elements, NS send hash result to NR
 *     NS:
 *       Header: [ptoId: hpsi, stepId, 2, senderId: NS, receiverId: NR, extraInfo: hashFuncId]
 *       Payload: [hash(ele_ns_i)]
 *   Step3:
 *     NR receives [hash(ele_ns_i)] from NS, and find the same elements in [hash(ele_nr_i)]
 */

public class HashPSI extends ProtocolExecutor {

  public HashPSI(Rpc rpc) {
    super(rpc, ProtocolType.HASH_PSI);
  }

  /*
   * determine who is sender in the following steps
   * party with smaller set is sender
   * when set size is equal, party with lower id is sender
   */
  Pair<Integer, Integer> getSenderReceiver(DataPacket initPacket) {
    DataPacketHeader initHeader = initPacket.getHeader();
    int ownId = rpc.ownParty().getPartyId();
    int otherId;
    if (initHeader.getSenderId() == ownId) {
      otherId = initHeader.getReceiverId();
    } else if (initHeader.getReceiverId() == ownId) {
      otherId = initHeader.getSenderId();
    } else {
      LOG.error("Illegal DataPacketHeader: party id not found in sender/recevier id");
      throw new RuntimeException("Illegal DataPacketHeader: party id not found in sender/recevier id");
    }
    int localSize = initPacket.getPayload().size();
    DataPacketHeader sendHeader = new DataPacketHeader(initHeader.getTaskId(), initHeader.getPtoId(), 1, initHeader.getExtraInfo(), ownId, otherId);
    rpc.send(DataPacket.fromByteArrayList(sendHeader, ImmutableList.of(OneDBCodec.encodeInt(localSize))));
    DataPacketHeader expect = new DataPacketHeader(initHeader.getTaskId(), initHeader.getPtoId(), 1, initHeader.getExtraInfo(), otherId, ownId);
    DataPacket setSizeResult = rpc.receive(expect);
    if (setSizeResult == null) {
      LOG.error("{} fail to get set size from party {} in HashPSI", rpc.ownParty(), otherId);
      throw new RuntimeException("Fail to get set size in HashPSI");
    } else {
      int remoteSize = OneDBCodec.decodeInt(setSizeResult.getPayload().get(0));
      if (remoteSize < localSize) {
        return Pair.of(otherId, ownId);
      } else if (remoteSize > localSize) {
        return Pair.of(ownId, otherId);
      } else {
        if (otherId < ownId) {
          return Pair.of(otherId, ownId);
        } else {
          return Pair.of(ownId, otherId);
        }
      }
    }
  }

  List<byte[]> hash4Sender(List<byte[]> ori, HashFunction func) {
    ImmutableList.Builder<byte[]> builder = ImmutableList.builder();
    for (byte[] row : ori) {
      builder.add(func.hash(row));
    }
    return builder.build();
  }

  Multimap<ByteBuffer, Integer> hash4Receiver(List<byte[]> ori, HashFunction func) {
    ImmutableMultimap.Builder<ByteBuffer, Integer> builder = ImmutableMultimap.builder();
    for (int i = 0; i < ori.size(); ++i) {
      byte[] key = func.hash(ori.get(i));
      builder.put(ByteBuffer.wrap(key), i);
    }
    return builder.build();
  }

  // sender hash local data, send them to receiver and wait for intersect result
  List<byte[]> senderProcedure(List<byte[]> localData, long taskId, int receiverId, HashFunction func) {
    List<byte[]> hashResult = hash4Sender(localData, func);
    DataPacketHeader senderHeader = new DataPacketHeader(taskId, type.getId(), 2, func.getId(), rpc.ownParty().getPartyId(), receiverId);
    rpc.send(DataPacket.fromByteArrayList(senderHeader, hashResult));
    DataPacketHeader expectHeader = new DataPacketHeader(taskId, type.getId(), 3, func.getId(), receiverId, rpc.ownParty().getPartyId());
    DataPacket psiResult = rpc.receive(expectHeader);
    if (psiResult == null) {
      LOG.error("Sender [{}] fail to get result from Receiver[{}] in HashPSI", rpc.ownParty(), receiverId);
      throw new RuntimeException("Fail to get result in HashPSI");
    } else {
      LOG.debug("{} get {} elements in HashPSI", rpc.ownParty(), psiResult.getPayload().size());
      return psiResult.getPayload();
    }
  }

  // receiver hash local data wait for sender's result, execute intersect and send result to sender
  List<byte[]> receiverProcedure(List<byte[]> localData, long taskId, int senderId, HashFunction func) {
    Multimap<ByteBuffer, Integer> receiverIndex = hash4Receiver(localData, func);
    DataPacketHeader expectHeader = new DataPacketHeader(taskId, type.getId(), 2, func.getId(), senderId, rpc.ownParty().getPartyId());
    DataPacket senderHashResult = rpc.receive(expectHeader);
    if (senderHashResult == null) {
      LOG.error("Receiver [{}] fail to get hash result from Sender[{}] in HashPSI", rpc.ownParty(), senderId);
      throw new RuntimeException("Fail to get hash result from sender in HashPSI");
    } else {
      List<byte[]> senderData = senderHashResult.getPayload();
      ImmutableList.Builder<byte[]> senderIntersect = ImmutableList.builder();
      ImmutableList.Builder<byte[]> receiverIntersect = ImmutableList.builder();
      for (int i = 0; i < senderData.size(); ++i) {
        ByteBuffer senderHash = ByteBuffer.wrap(senderData.get(i));
        if (receiverIndex.containsKey(senderHash)) {
          for (Integer receiverId :receiverIndex.get(senderHash)) {
            senderIntersect.add(OneDBCodec.encodeInt(receiverId));
            receiverIntersect.add(OneDBCodec.encodeInt(i));
          }
        }
      }
      DataPacketHeader resultHeader = new DataPacketHeader(taskId, type.getId(), 3, func.getId(), rpc.ownParty().getPartyId(), senderId);
      rpc.send(DataPacket.fromByteArrayList(resultHeader, senderIntersect.build()));
      List<byte[]> receiverResult = receiverIntersect.build();
      LOG.debug("{} get {} elements in HashPSI", rpc.ownParty(), receiverResult.size());
      return receiverResult;
    }
  }

  @Override
  public List<byte[]> run(DataPacket initPacket) {
    Pair<Integer, Integer> senderReceiver = getSenderReceiver(initPacket);
    int sender = senderReceiver.getLeft();
    int receiver = senderReceiver.getRight();
    List<byte[]> localData = initPacket.getPayload();
    long taskId = initPacket.getHeader().getTaskId();
    HashFunction hashFunc = HashFunction.of((int) initPacket.getHeader().getExtraInfo());
    LOG.debug("Use {} in HashPSI", hashFunc);
    if (sender == rpc.ownParty().getPartyId()) {
      LOG.debug("{} is the sender of HashPSI", rpc.ownParty());
      return senderProcedure(localData, taskId, receiver, hashFunc);
    } else if (receiver == rpc.ownParty().getPartyId()) {
      LOG.debug("{} is the receiver of HashPSI", rpc.ownParty());
      return receiverProcedure(localData, taskId, sender, hashFunc);
    } else {
      LOG.error("Fail to determine who is sender/receiver in HashPSI");
      throw new RuntimeException("Fail to determine who is sender/receiver in HashPSI");
    }
  }
}