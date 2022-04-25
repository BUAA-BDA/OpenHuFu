package com.hufudb.onedb.mpc.ot;

import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;

/*
 * Public key based OT 1-out-of-n implementation
 * Participants: S and R
 * Init DataPacket:
 *   S:
 *     Header: [ptoId: pkot, stepId: 0, senderId: R, recieveId: S, extraInfo: n]
 *     Payload: n secret values [x_0, x_1, ..., x_n-1]
 *   R:
 *     Header: [ptoId: pkot, stepId: 0, senderId: R, recieveId: S, extraInfo: select bits b]
 *     Payload: null
 * Step1:
 *   R generates (sk, pk) and n - 1 random pk', sends [pk_0, pk_1, .. pk_n-1]to S (pk_b = pk and others are filled with pk')
 *   Send DataPacket Format:
 *   Header: [ptoId: pkot, stepId: 1, senderId: R, recieveId: S, extraInfo: n]
 *   Payload: [pk_0, ..., pk_n-1]
 * Step2:
 *   S receives pk list, for each x_i, encrypts it with pk_i as e_i, and sends [e_0, ..., e_n-1] to R
 *   Send DataPacket Format:
 *   Header: [ptoId, pkot, stepId: 2, senderId, S, receivedId: R, extraInfo: n]
 *   Payload: [e_0, ..., e_n-1]
 * Step3:
 *   R receives e list, and uses sk to decrypt e_b and get the value
 *   Result DataPacket Format:
 *   Header: [ptoId, pkot, stepId: 3]
 *   Payload: [x_b]
 */

public class PublicKeyOT extends ProtocolExecutor {

  PublicKeyOT(Rpc rpc) {
    super(rpc, ProtocolType.PK_OT);
  }

  DataPacketHeader step1(DataPacket packet) {
    return null;
  }

  DataPacketHeader step2(DataPacket packet) {
    return null;
  }

  DataPacketHeader step3(DataPacket packet) {
    return null;
  }

  void initSender(DataPacket packet) {
    
  }

  void initReceiver(DataPacket receiver) {

  }

  @Override
  public DataPacketHeader run(DataPacket initPacket) {
    DataPacketHeader header = initPacket.getHeader();
    assert header.getPtoId() == type.getId();
    switch(header.getStepId()) {
      case 0: return step1(initPacket);
      case 1: return step2(initPacket);
      case 2: return step3(initPacket);
      default:
        LOG.error("Invalid packet header for {}", type.toString());
    }
    return null;
  }

  public DataPacket getResult(DataPacketHeader header) {
    // Rpc rpc = manager.getRpc(header.getSenderId());
    return null;
  }
}
