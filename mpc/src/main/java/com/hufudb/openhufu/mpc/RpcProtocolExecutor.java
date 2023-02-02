package com.hufudb.openhufu.mpc;

import com.hufudb.openhufu.mpc.random.BasicRandom;
import com.hufudb.openhufu.mpc.random.OpenHuFuRandom;
import com.hufudb.openhufu.rpc.Rpc;

public abstract class RpcProtocolExecutor implements ProtocolExecutor {
    protected static final OpenHuFuRandom random = new BasicRandom();
  
    final protected Rpc rpc;
    final protected ProtocolType type;
    final protected int ownId;
  
    protected RpcProtocolExecutor(Rpc rpc, ProtocolType type) {
      this.rpc = rpc;
      this.type = type;
      this.ownId = rpc.ownParty().getPartyId();
    }
  
    public ProtocolType getProtocolType() {
      return type;
    }
  
    public int getProtocolTypeId() {
      return type.getId();
    }
  
    public int getOwnId() {
      return ownId;
    }
  }
  
