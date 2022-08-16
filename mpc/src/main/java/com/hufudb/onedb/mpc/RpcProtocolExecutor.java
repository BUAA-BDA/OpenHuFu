package com.hufudb.onedb.mpc;

import com.hufudb.onedb.mpc.random.BasicRandom;
import com.hufudb.onedb.mpc.random.OneDBRandom;
import com.hufudb.onedb.rpc.Rpc;

public abstract class RpcProtocolExecutor implements ProtocolExecutor {
    protected static final OneDBRandom random = new BasicRandom();
  
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
  
