package com.hufudb.onedb.mpc;

import com.hufudb.onedb.rpc.RpcManager;
import com.hufudb.onedb.rpc.utils.DataPacket;

public abstract class ProtocolExecutor {
  final RpcManager manager;
  final ProtocolType type;

  protected ProtocolExecutor(RpcManager manager, ProtocolType type) {
    this.manager = manager;
    this.type = type;
  }

  public ProtocolType getProtocolType() {
    return type;
  }

  public abstract void execute(DataPacket packet);
}
