package com.hufudb.onedb.mpc.ot;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.elgamal.Elgamal;
import com.hufudb.onedb.mpc.elgamal.ElgamalFactory;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;

import java.util.ArrayList;
import java.util.List;

/*-
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

public class PublicKeyOT extends ProtocolExecutor {

  public PublicKeyOT(Rpc rpc) {
    super(rpc, ProtocolType.PK_OT);
  }

  // step 1, run on R
  DataPacketHeader generateKeys(OTMeta meta) {
    int m = OneDBCodec.decodeInt(meta.secrets.get(0));
    int n = 1 << m;
    int mask = n - 1;
    int b = OneDBCodec.decodeInt(meta.secrets.get(1)) & mask;
    LOG.debug("{} generate [{}] public keys, a private key for [{}]", rpc.ownParty(), n, b);
    List<byte[]> payloads = new ArrayList<>();
    Elgamal privateKey = ElgamalFactory.createElgamal(true);
    payloads.add(privateKey.getPByteArray());
    payloads.add(privateKey.getGByteArray());
    try {
      for (int i = 0; i < n; ++i) {
          if (i == b) {
            payloads.add(privateKey.getPublicKey());
          } else {
            payloads.add(privateKey.generatePseudoPublicKey());
          }

      }
    } catch (Exception e) {
      LOG.error("Error when generating key pair: {}", e.getMessage());
      return null;
    }
    meta.b = b;
    meta.key = privateKey;
    DataPacketHeader outHeader = new DataPacketHeader(meta.taskId, ProtocolType.PK_OT.getId(), 1,
        meta.extraInfo, meta.ownId, meta.otherId);
    rpc.send(DataPacket.fromByteArrayList(outHeader, payloads));
    LOG.debug("{} send {}", rpc.ownParty(), outHeader);
    // waiting for receving a packet with below header
    DataPacketHeader expect = new DataPacketHeader(meta.taskId, ProtocolType.PK_OT.getId(), 2,
        meta.extraInfo, meta.otherId, meta.ownId);
    LOG.debug("{} wait for packet {}", rpc.ownParty(), expect);
    return expect;
  }

  // step 2, run on S
  DataPacketHeader encryptSecrets(DataPacket packet, OTMeta meta) {
    DataPacketHeader header = packet.getHeader();
    List<byte[]> secrets = meta.secrets;
    List<byte[]> publicKeyBytes = packet.getPayload();
    int n = publicKeyBytes.size() - 2;
    List<byte[]> encryptedSecrets = new ArrayList<>();
    LOG.debug("{} encrypts secrets with public keys from Party [{}]", rpc.ownParty(),
        header.getSenderId());
    try {
      for (int i = 0; i < n; ++i) {
        Elgamal elgamal = ElgamalFactory.createElgamal(publicKeyBytes.get(0), publicKeyBytes.get(1), publicKeyBytes.get(i + 2));
        encryptedSecrets.add(elgamal.encrypt(secrets.get(i)));
      }
    } catch (Exception e) {
      LOG.error("Error when encrypting: {}", e.getMessage());
      e.printStackTrace();
      return null;
    }
    DataPacketHeader outHeader = new DataPacketHeader(header.getTaskId(), header.getPtoId(), 2,
        meta.extraInfo, header.getReceiverId(), header.getSenderId());
    rpc.send(DataPacket.fromByteArrayList(outHeader, encryptedSecrets));
    LOG.debug("{} send {}", rpc.ownParty(), outHeader);
    return null;
  }

  // step3, run on R
  List<byte[]> decryptSecrets(DataPacket packet, OTMeta meta) {
    Elgamal key = meta.key;
    int b = meta.b;
    byte[] target = packet.getPayload().get(b);
    byte[] decryptedBytes = null;
    LOG.debug("{} decrypt secret [{}]", rpc.ownParty(), b);
    try {
      decryptedBytes = key.decrypt(target);
    } catch (Exception e) {
      LOG.error("Error when decrypting: {}", e.getMessage());
    }
    return ImmutableList.of(decryptedBytes);
  }

  List<byte[]> senderProcedure(OTMeta meta) {
    LOG.debug("{} is sender of OT", rpc.ownParty());
    DataPacketHeader expect = new DataPacketHeader(meta.taskId, ProtocolType.PK_OT.getId(), 1,
        meta.extraInfo, meta.otherId, meta.ownId);
    encryptSecrets(rpc.receive(expect), meta);
    return ImmutableList.of();
  }

  List<byte[]> receiverProcedure(OTMeta meta) {
    LOG.debug("{} is receiver of OT", rpc.ownParty());
    DataPacketHeader expectHeader = generateKeys(meta);
    return decryptSecrets(rpc.receive(expectHeader), meta);
  }

  // Sender: the party who has n secrets
  boolean isSender(DataPacketHeader initHeader) {
    return initHeader.getSenderId() == rpc.ownParty().getPartyId();
  }

  @Override
  public List<byte[]> run(long taskId, List<Integer> parties, List<byte[]> inputData,
      Object... args) {
    // DataPacketHeader header = initPacket.getHeader();

    // assert header.getPtoId() == type.getId();
    int senderId = (Integer) args[0];
    int receiverId = (Integer) args[1];
    long extraInfo = 0L;
    if (args.length == 3) {
      extraInfo = (Long) args[2];
    }
    int ownId = rpc.ownParty().getPartyId();
    if (senderId == ownId) {
      return senderProcedure(new OTMeta(taskId, ownId, receiverId, extraInfo, inputData));
    } else if (receiverId == ownId) {
      return receiverProcedure(new OTMeta(taskId, ownId, senderId, extraInfo, inputData));
    } else {
      LOG.error("{} is not participant of Publickey OT", rpc.ownParty());
      throw new RuntimeException("Not participant of PK_OT");
    }
  }

  static class OTMeta {
    long taskId;
    int b;
    int ownId;
    int otherId;
    long extraInfo;
    Elgamal key;
    List<byte[]> secrets;

    OTMeta() {}

    OTMeta(int b, Elgamal key) {
      this.b = b;
      this.key = key;
    }

    OTMeta(long taskId, int ownId, int otherId, long extraInfo, List<byte[]> inputs) {
      this.taskId = taskId;
      this.ownId = ownId;
      this.otherId = otherId;
      this.extraInfo = extraInfo;
      this.secrets = inputs;
    }

    OTMeta(List<byte[]> secrets) {
      this.secrets = secrets;
    }
  }
}
