package com.hufudb.onedb.mpc.psi;

import java.util.List;
import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;

/*-
 * Hash based PSI implementation
 *   Participants: S and R
 *   Init DataPacket
 *     S:
 *       Header: [ptoId: hpsi, stepId, 0, senderId: S, receiverId: R]
 *       Payload: [hashFunctionId, salt, byte array of keys]
 *     R:
 *       Header: [ptoId: hpsi, stepId, 0, senderId: S, receiverId: R]
 *       Payload: [hashFunctionId, salt, byte array of keys]
 */

public class HashPSI extends ProtocolExecutor {

  public HashPSI(Rpc rpc) {
    super(rpc, ProtocolType.HASH_PSI);
  }

  @Override
  public List<byte[]> run(DataPacket initPacket) {
    return null;
  }
}
