package com.hufudb.onedb.mpc;

import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public abstract class ProtocolExecutor {
  protected static final Logger LOG = LoggerFactory.getLogger(ProtocolExecutor.class);

  final protected Rpc rpc;
  final protected ProtocolType type;

  protected ProtocolExecutor(Rpc rpc, ProtocolType type) {
    this.rpc = rpc;
    this.type = type;
  }

  public ProtocolType getProtocolType() {
    return type;
  }

  public abstract DataPacketHeader run(DataPacket initPacket);

}
