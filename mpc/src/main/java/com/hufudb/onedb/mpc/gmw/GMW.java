package com.hufudb.onedb.mpc.gmw;

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.bristol.BristolFile;
import com.hufudb.onedb.mpc.ot.PublicKeyOT;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;

/*-
 * GMW implementation
 *   Participants: A and B (A < B)
 *   Init DataPacket:
 *     A:
 *       Header: [ptoId: gmw, stepId: 0, senderId: A, receiverId: B, extraInfo: opType]
 *       DataPacket: [inputBytes]
 *     B:
 *       Header: [ptoId: gmw, stepId: 0, senderId: B, receiverId: A, extraInfo: opType]
 *       DataPacket: [inputBytes]
 *   Step1: A and B load corresponding Bristol format file of opType and cache the file,
 *         share inputBytes to each other and cache local bytes
 *     Send DataPacket Format for A/B:
 *       Header: [ptoId, gmw, stepId: 1, senderId: A/B, recieverId: B/A, extraInfo: opType]
 */
public class GMW extends ProtocolExecutor {
  final PublicKeyOT otExecutor;
  private final Map<Long, GMWCache> cache = new ConcurrentHashMap<>();

  protected GMW(Rpc rpc, PublicKeyOT otExecutor) {
    super(rpc, ProtocolType.GMW);
    this.otExecutor = otExecutor;
  }

  void prepare(DataPacket initPacket) {
    
  }

  void andGate() {

  }

  @Override
  public DataPacket run(DataPacket initPacket) {
    return null;
  }

  static class GMWCache {
    BristolFile bristol;
    BitSet bitSet;
  }
}
