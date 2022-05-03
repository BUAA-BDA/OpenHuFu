package com.hufudb.onedb.mpc;

import java.util.List;
import com.hufudb.onedb.mpc.random.BasicRandom;
import com.hufudb.onedb.mpc.random.OneDBRandom;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ProtocolExecutor {
  protected static final Logger LOG = LoggerFactory.getLogger(ProtocolExecutor.class);
  protected static final OneDBRandom random = new BasicRandom();

  final protected Rpc rpc;
  final protected ProtocolType type;

  protected ProtocolExecutor(Rpc rpc, ProtocolType type) {
    this.rpc = rpc;
    this.type = type;
  }

  public ProtocolType getProtocolType() {
    return type;
  }

  // Beta API, parameters may change to [Set<Integer> partySet, List<byte[]> inputData, int extraInfo]
  public abstract List<byte[]> run(DataPacket initPacket);
}
