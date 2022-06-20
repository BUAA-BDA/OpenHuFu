package com.hufudb.onedb.mpc.ot;

import com.hufudb.onedb.mpc.ProtocolException;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.RpcProtocolExecutor;
import com.hufudb.onedb.mpc.elgamal.Elgamal;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import java.util.ArrayList;
import java.util.List;

/**
 * Public key based OT 1-out-of-n implementation
 *   Participants: S and R
 *   Init DataPacket:
 *     S:
 *       Header: [ptoId: pkot, stepId: 0, senderId: S, recieveId: R, extraInfo: flag(e.g. the gate id in circuit)]
 *       Payload: n secret values [x_0, x_1, ..., x_n-1]
 *     R: 
 *       Header: [ptoId: pkot, stepId: 0, senderId: S, recieveId: R, extraInfo: flag]
 *       Payload: [select bits length, select bits b]
 *   Step1: R generates (sk, pk) and n - 1 random pk', sends [pk_0, pk_1, .. pk_n-1]to S (pk_b = pk and others are filled with pk')
 *     Send DataPacket Format:
 *       Header: [ptoId: pkot, stepId: 1, senderId: R, recieveId: S, extraInfo: flag] Payload: [pk_0, ..., pk_n-1]
 *   Step2: S receives pk list, for each x_i, encrypts it with pk_i as e_i, and sends [e_0, ..., e_n-1] to R
 *     Send DataPacket Format: 
 *       Header: [ptoId, pkot, stepId: 2, senderId, S, receivedId: R, extraInfo: flag] Payload: [e_0, ..., e_n-1]
 *   Step3: R receives e list, and uses sk to decrypt e_b and get the value
 *     Result DataPacket Format: Header: [ptoId, pkot, stepId: 3]
 *    Payload: [x_b]
 */

public class PublicKeyOT extends RpcProtocolExecutor {

  public PublicKeyOT(Rpc rpc) {
    super(rpc, ProtocolType.PK_OT);
  }

  // step 1, run on R
  Elgamal generateKeys(long taskId, int senderId, int sel, int exp, long extraInfo) {
    int n = 1 << exp;
    LOG.debug("{} generate [{}] public keys, a private key for [{}]", rpc.ownParty(), n, sel);
    List<byte[]> payloads = new ArrayList<>();
    Elgamal privateKey = Elgamal.create(true);
    payloads.add(privateKey.getPByteArray());
    payloads.add(privateKey.getGByteArray());
    try {
      for (int i = 0; i < n; ++i) {
        if (i == sel) {
          payloads.add(privateKey.getPublicKey());
        } else {
          payloads.add(privateKey.generatePseudoPublicKey());
        }
      }
    } catch (Exception e) {
      LOG.error("Error when generating key pair: {}", e.getMessage());
      return null;
    }
    DataPacketHeader outHeader =
        new DataPacketHeader(taskId, ProtocolType.PK_OT.getId(), 1, extraInfo, ownId, senderId);
    rpc.send(DataPacket.fromByteArrayList(outHeader, payloads));
    LOG.debug("{} send {}", rpc.ownParty(), outHeader);
    return privateKey;
  }

  // step 2, run on S
  DataPacketHeader encryptSecrets(DataPacket packet, List<?> secrets, long extraInfo) {
    DataPacketHeader header = packet.getHeader();
    List<byte[]> publicKeyBytes = packet.getPayload();
    int n = publicKeyBytes.size() - 2;
    List<byte[]> encryptedSecrets = new ArrayList<>();
    LOG.debug("{} encrypts secrets with public keys from Party [{}]", rpc.ownParty(),
        header.getSenderId());
    try {
      for (int i = 0; i < n; ++i) {
        Elgamal elgamal =
            Elgamal.create(publicKeyBytes.get(0), publicKeyBytes.get(1), publicKeyBytes.get(i + 2));
        encryptedSecrets.add(elgamal.encrypt((byte[]) secrets.get(i)));
      }
    } catch (Exception e) {
      LOG.error("Error when encrypting: {}", e.getMessage());
      e.printStackTrace();
      return null;
    }
    DataPacketHeader outHeader = new DataPacketHeader(header.getTaskId(), header.getPtoId(), 2,
        extraInfo, header.getReceiverId(), header.getSenderId());
    rpc.send(DataPacket.fromByteArrayList(outHeader, encryptedSecrets));
    LOG.debug("{} send {}", rpc.ownParty(), outHeader);
    return null;
  }

  // step3, run on R
  byte[] decryptSecrets(DataPacket packet, Elgamal privateKey, int sel) throws ProtocolException {
    Elgamal key = privateKey;
    byte[] target = packet.getPayload().get(sel);
    byte[] decryptedBytes = null;
    LOG.debug("{} decrypt secret [{}]", rpc.ownParty(), sel);
    try {
      decryptedBytes = key.decrypt(target);
    } catch (Exception e) {
      LOG.error("Error when decrypting: {}", e.getMessage());
      throw new ProtocolException("Decryption error at receiver of PublicKyeOT", e);
    }
    return decryptedBytes;
  }

  /**
   * @param args List<byte[]> inputdata (required), Long extraInfo (optional)
   * @return
   */
  Object senderProcedure(long taskId, int receivedId, List<byte[]> inputData, long extraInfo) throws ProtocolException {
    LOG.debug("{} is sender of OT", rpc.ownParty());
    DataPacketHeader expect =
        new DataPacketHeader(taskId, ProtocolType.PK_OT.getId(), 1, extraInfo, receivedId, ownId);
    encryptSecrets(rpc.receive(expect), inputData, extraInfo);
    return null;
  }

  byte[] receiverProcedure(long taskId, int senderId, int sel, int exp, long extraInfo) throws ProtocolException {
    LOG.debug("{} is receiver of OT", rpc.ownParty());
    sel = sel & ((1 << exp) - 1);
    Elgamal privatekey = generateKeys(taskId, senderId, sel, exp, extraInfo);
    DataPacketHeader expect =
        new DataPacketHeader(taskId, ProtocolType.PK_OT.getId(), 2, extraInfo, senderId, ownId);
    LOG.debug("{} wait for packet {}", rpc.ownParty(), expect);
    return decryptSecrets(rpc.receive(expect), privatekey, sel);
  }

  /**
   * @param parties {senderId, receiverId}
   * @param args[0] List<byte[]> inputdata for sender (required), or int sel for receiver (required)
   * @param args[1] long extraInfo for sender (optional), or int exp for receiver (required)
   * @param args[2] long extraInfo for receiver (optional)
   * @return null for sender, byte[] for receiver
   */
  @Override
  public Object run(long taskId, List<Integer> parties, Object... args) throws ProtocolException {
    if (args.length < 1) {
      LOG.error("PublickeyOT requires args List<byte[]> for sender and int receiver");
      return null;
    }
    int senderId = parties.get(0);
    int receiverId = parties.get(1);
    long extraInfo = 0;
    if (senderId == ownId) {
      if (args.length > 1) {
        extraInfo = (long) args[1];
      }
      return senderProcedure(taskId, receiverId, (List<byte[]>) args[0], extraInfo);
    } else {
      if (args.length > 2) {
        extraInfo = (long) args[2];
      }
      return receiverProcedure(taskId, senderId, (int) args[0],(int) args[1], extraInfo);
    }
  }
}
