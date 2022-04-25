package com.hufudb.onedb.rpc.grpc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.google.common.annotations.VisibleForTesting;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.grpc.concurrent.ConcurrentBuffer;
import com.hufudb.onedb.rpc.grpc.pipe.PipeClient;
import com.hufudb.onedb.rpc.grpc.pipe.PipeService;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import io.grpc.BindableService;
import io.grpc.Channel;

public class OneDBRpc implements Rpc {
  private static int THREAD_NUM = 8;
  final Party own;
  final Set<Party> parties;
  final Map<Integer, Party> participantMap;
  final Map<Integer, PipeClient> clientMap;
  final Map<Integer, ConcurrentBuffer> bufferMap;
  final PipeService gRpcService;
  final ExecutorService execService;
  long payloadByteLength;
  long dataPacketNum;

  public OneDBRpc(Party own, Set<Party> parties) {
    this.own = own;
    this.parties = parties;
    this.participantMap = new HashMap<>();
    this.clientMap = new HashMap<>();
    this.bufferMap = new HashMap<>();
    this.execService = Executors.newFixedThreadPool(THREAD_NUM);
    for (Party p : parties) {
      this.participantMap.put(p.getPartyId(), p);
      if (!p.equals(own)) {
        this.clientMap.put(p.getPartyId(), new PipeClient(own.getPartyName()));
        this.bufferMap.put(p.getPartyId(), new ConcurrentBuffer());
      }
    }
    this.gRpcService = new PipeService(bufferMap);
    this.payloadByteLength = 0;
    this.dataPacketNum = 0;
  }

  @VisibleForTesting
  public OneDBRpc(Party own, List<Party> parties, List<Channel> channels) {
    assert parties.size() == channels.size();
    this.own = own;
    this.parties = new HashSet<>();
    this.participantMap = new HashMap<>();
    this.clientMap = new HashMap<>();
    this.bufferMap = new HashMap<>();
    for (int i = 0; i < parties.size(); ++i) {
      Party p = parties.get(i);
      Channel ch = channels.get(i);
      this.parties.add(p);
      this.participantMap.put(p.getPartyId(), p);
      if (!p.equals(own)) {
        this.clientMap.put(p.getPartyId(), new PipeClient(ch));
        this.bufferMap.put(p.getPartyId(), new ConcurrentBuffer());
      }
    }
    this.gRpcService = new PipeService(bufferMap);
    this.execService = Executors.newFixedThreadPool(THREAD_NUM);
    this.payloadByteLength = 0;
    this.dataPacketNum = 0;
  }

  @Override
  public Party ownParty() {
    return own;
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
    clientMap.values().stream().forEach(c -> c.connect());
  }

  @Override
  public void send(DataPacket dataPacket) {
    payloadByteLength += dataPacket.getPayloadByteLength();
    PipeClient client = clientMap.get(dataPacket.getHeader().getReceiverId());
    client.send(dataPacket.toProto());
  }

  @Override
  public DataPacket receive(DataPacketHeader header) {
    ConcurrentBuffer buffer = bufferMap.get(header.getSenderId());
    return buffer.blockingPop(header);
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
    clientMap.values().forEach(c -> c.close());
  }

  public BindableService getgRpcService() {
    return gRpcService;
  }
}
