package com.hufudb.onedb.rpc.grpc;

import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.hufudb.onedb.rpc.Party;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.RpcManager;
import com.hufudb.onedb.rpc.grpc.pipe.PipeService;
import com.hufudb.onedb.rpc.grpc.queue.ConcurrentBuffer;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneDBRpcManager implements RpcManager {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBRpcManager.class);

  final Party self;
  final Set<Party> parties;
  final Map<Integer, Rpc> rpcMap;
  final Map<Integer, ConcurrentBuffer> receiveBuffers;
  final PipeService service;
  // final int port;
  // final Server server;

  public OneDBRpcManager(Set<Party> parties, Party self) {
    this.self = self;
    this.parties = parties;
    ImmutableMap.Builder<Integer, Rpc> rpcMapBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<Integer, ConcurrentBuffer> bufferMapBuilder = ImmutableMap.builder();
    for (Party pt : parties) {
      ConcurrentBuffer buffer = new ConcurrentBuffer();
      bufferMapBuilder.put(pt.getPartyId(), buffer);
      rpcMapBuilder.put(pt.getPartyId(), new OneDBRpc(self, parties, buffer));
    }
    this.rpcMap = rpcMapBuilder.build();
    this.receiveBuffers = bufferMapBuilder.build();
    this.service = new PipeService(receiveBuffers);
    // this.port = Integer.valueOf(self.getPartyName().split(":", 0)[1]);
    // this.server = ServerBuilder.forPort(this.port).build();
  }

  @VisibleForTesting
  public OneDBRpcManager(List<Party> parties, List<Channel> channels, Party self) {
    assert parties.size() == channels.size();
    this.self = self;
    this.parties = ImmutableSet.copyOf(parties);
    ImmutableMap.Builder<Integer, Rpc> rpcMapBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<Integer, ConcurrentBuffer> bufferMapBuilder = ImmutableMap.builder();
    for (int i = 0; i < parties.size(); ++i) {
      Party pt = parties.get(i);
      if (pt.equals(self)) {
        continue;
      }
      Channel chan = channels.get(i);
      ConcurrentBuffer buffer = new ConcurrentBuffer();
      bufferMapBuilder.put(pt.getPartyId(), buffer);
      rpcMapBuilder.put(pt.getPartyId(), new OneDBRpc(self, this.parties, buffer, chan));
    }
    this.rpcMap = rpcMapBuilder.build();
    this.receiveBuffers = bufferMapBuilder.build();
    this.service = new PipeService(receiveBuffers);
  }

  @Override
  public Rpc getRpc(int partyId) {
    return rpcMap.get(partyId);
  }

  @Override
  public int getPartyNum() {
    return rpcMap.size();
  }

  @Override
  public Set<Party> getPartySet() {
    return parties;
  }

  public BindableService getgRpcService() {
    return service;
  }
}
