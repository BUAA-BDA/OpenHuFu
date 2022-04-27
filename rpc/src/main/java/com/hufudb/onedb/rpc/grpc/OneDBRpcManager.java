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
import io.grpc.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneDBRpcManager implements RpcManager {
  protected static final Logger LOG = LoggerFactory.getLogger(OneDBRpcManager.class);

  final Set<Party> parties;
  final Map<Integer, Rpc> rpcMap;

  public OneDBRpcManager(Set<Party> parties) {
    this.parties = parties;
    ImmutableMap.Builder<Integer, Rpc> rpcMapBuilder = ImmutableMap.builder();
    for (Party pt : parties) {
      rpcMapBuilder.put(pt.getPartyId(), new OneDBRpc(pt, parties));
    }
    this.rpcMap = rpcMapBuilder.build();
  }

  @VisibleForTesting
  public OneDBRpcManager(List<Party> parties, List<Channel> channels) {
    assert parties.size() == channels.size();
    this.parties = ImmutableSet.copyOf(parties);
    ImmutableMap.Builder<Integer, Rpc> rpcMapBuilder = ImmutableMap.builder();
    for (int i = 0; i < parties.size(); ++i) {
      Party pt = parties.get(i);
      rpcMapBuilder.put(pt.getPartyId(), new OneDBRpc(pt, parties, channels));
    }
    this.rpcMap = rpcMapBuilder.build();
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
}
