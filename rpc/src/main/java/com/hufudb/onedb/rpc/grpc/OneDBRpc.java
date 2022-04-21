package com.hufudb.onedb.rpc.grpc;

import java.util.Map;
import java.util.Set;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.grpc.pipe.PipeClient;
import com.hufudb.onedb.rpc.grpc.queue.ConcurrentBuffer;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import io.grpc.Channel;

public class OneDBRpc implements Rpc {

  final Party target;
  final Set<Party> parties;
  final Map<Integer, Party> participantMap;
  final ConcurrentBuffer receiveBuffer;
  final PipeClient client;
  long payloadByteLength;
  long dataPacketNum;

  public OneDBRpc(Party target, Set<Party> parties, ConcurrentBuffer receiveBuffer) {
    this.target = target;
    this.parties = parties;
    ImmutableMap.Builder<Integer, Party> builder = ImmutableMap.builder();
    for (Party p : parties) {
      builder.put(p.getPartyId(), p);
    }
    this.participantMap = builder.build();
    this.receiveBuffer = receiveBuffer;
    this.client = new PipeClient(target.getPartyName());
    this.payloadByteLength = 0;
    this.dataPacketNum = 0;
  }

  @VisibleForTesting
  public OneDBRpc(Party target, Set<Party> parties, ConcurrentBuffer receiveBuffer, Channel channel) {
    this.target = target;
    this.parties = parties;
    ImmutableMap.Builder<Integer, Party> builder = ImmutableMap.builder();
    for (Party p : parties) {
      builder.put(p.getPartyId(), p);
    }
    this.participantMap = builder.build();
    this.receiveBuffer = receiveBuffer;
    this.client = new PipeClient(channel);
    this.payloadByteLength = 0;
    this.dataPacketNum = 0;
  }

  @Override
  public Party ownParty() {
    return target;
  }

  @Override
  public Set<Party> getPartySet() {
    return parties;
  }

  @Override
  public Party getParty(int partyId) {
    return participantMap.get(partyId);
  }

  @Override
  public void connect() {
    client.connect();
  }

  @Override
  public void send(DataPacket dataPacket) {
    payloadByteLength += dataPacket.getPayloadByteLength();
    this.client.send(dataPacket.toProto());
  }

  @Override
  public DataPacket receive(DataPacketHeader header) {
    return receiveBuffer.get(header);
  }

  @Override
  public long getPayloadByteLength(boolean reset) {
    long length = payloadByteLength;
    if (reset) {
      payloadByteLength = 0;
    }
    return length;
  }

  @Override
  public long getSendDataPacketNum(boolean reset) {
    long number = dataPacketNum;
    if (reset) {
      dataPacketNum = 0;
    }
    return number;
  }

  @Override
  public void disconnect() {
    client.close();
  }
}
