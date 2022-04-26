package com.hufudb.onedb.mpc.ot;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;
import javax.crypto.Cipher;

/*-
 * Public key based OT 1-out-of-n implementation
 *   Participants: S and R
 *   Init DataPacket:
 *     S:
 *       Header: [ptoId: pkot, stepId: 0, senderId: S, recieveId: R, extraInfo: n]
 *       Payload: n secret values [x_0, x_1, ..., x_n-1] 
 *     R: 
 *       Header: [ptoId: pkot, stepId: 0, senderId: S, recieveId: R, extraInfo: select bits length m]
 *       Payload: select bits b
 *   Step1: R generates (sk, pk) and n - 1 random pk', sends [pk_0, pk_1, .. pk_n-1]to S (pk_b = pk and others are filled with pk')
 *     Send DataPacket Format:
 *       Header: [ptoId: pkot, stepId: 1, senderId: R, recieveId: S, extraInfo: n] Payload: [pk_0, ..., pk_n-1]
 *   Step2: S receives pk list, for each x_i, encrypts it with pk_i as e_i, and sends [e_0, ..., e_n-1] to R
 *     Send DataPacket Format: 
 *       Header: [ptoId, pkot, stepId: 2, senderId, S, receivedId: R, extraInfo: n] Payload: [e_0, ..., e_n-1]
 *   Step3: R receives e list, and uses sk to decrypt e_b and get the value
 *     Result DataPacket Format: Header: [ptoId, pkot, stepId: 3]
 *    Payload: [x_b]
 */

public class PublicKeyOT extends ProtocolExecutor {

  public PublicKeyOT(Rpc rpc) {
    super(rpc, ProtocolType.PK_OT);
  }

  // step 0, run on S
  DataPacketHeader storeSecrets(DataPacket packet, OTMeta meta) {
    DataPacketHeader header = packet.getHeader();
    meta.secrets = packet.getPayload();
    LOG.debug("Party [{}] store secrets", rpc.ownParty());
    DataPacketHeader expect = new DataPacketHeader(header.getTaskId(), header.getPtoId(), 1,
        meta.secrets.size(), header.getReceiverId(), header.getSenderId());
    LOG.debug("Party [{}] wait for packet {}", rpc.ownParty(), expect);
    return expect;
  }

  // step 1, run on R
  DataPacketHeader generateKeys(DataPacket packet, OTMeta meta) {
    DataPacketHeader header = packet.getHeader();
    int m = (int) header.getExtraInfo();
    int n = 1 << m;
    int mask = n - 1;
    int b = OneDBCodec.decodeInt(packet.getPayload().get(0)) & mask;
    LOG.debug("Party [{}] generate [{}] public keys, a private key for [{}]", rpc.ownParty(), n, b);
    List<byte[]> payloads = new ArrayList<>();
    PrivateKey privateKey = null;
    try {
      for (int i = 0; i < n; ++i) {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair pair = generator.generateKeyPair();
        payloads.add(pair.getPublic().getEncoded());
        if (i == b) {
          privateKey = pair.getPrivate();
        }
      }
    } catch (Exception e) {
      LOG.error("Error when generating key pair: {}", e.getMessage());
      return null;
    }
    meta.b = b;
    meta.key = privateKey;
    DataPacketHeader outHeader = new DataPacketHeader(header.getTaskId(), header.getPtoId(), 1, n,
        header.getReceiverId(), header.getSenderId());
    rpc.send(DataPacket.fromByteArrayList(outHeader, payloads));
    LOG.debug("Party [{}] send {}", rpc.ownParty(), outHeader);
    // waiting for receving a packet with below header
    DataPacketHeader expect = new DataPacketHeader(header.getTaskId(), header.getPtoId(), 2, n,
        outHeader.getReceiverId(), outHeader.getSenderId());
    LOG.debug("Party [{}] wait for packet {}", rpc.ownParty(), expect);
    return expect;
  }

  // step 2, run on S
  DataPacketHeader encryptSecrets(DataPacket packet, OTMeta meta) {
    DataPacketHeader header = packet.getHeader();
    List<byte[]> secrets = meta.secrets;
    int n = (int) header.getExtraInfo();
    List<byte[]> publicKeyBytes = packet.getPayload();
    List<byte[]> encryptedSecrets = new ArrayList<>();
    KeyFactory keyFactory = null;
    LOG.debug("Party [{}] encrypts secrets with public keys from Party [{}]",
        rpc.ownParty(), header.getSenderId());
    try {
      keyFactory = KeyFactory.getInstance("RSA");
      for (int i = 0; i < n; ++i) {
        EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes.get(i));
        PublicKey key = keyFactory.generatePublic(publicKeySpec);
        Cipher encryptCipher = Cipher.getInstance("RSA");
        encryptCipher.init(Cipher.ENCRYPT_MODE, key);
        encryptedSecrets.add(encryptCipher.doFinal(secrets.get(i)));
      }
    } catch (Exception e) {
      LOG.error("Error when encrypting: {}", e.getMessage());
      e.printStackTrace();
      return null;
    }
    DataPacketHeader outHeader = new DataPacketHeader(header.getTaskId(), header.getPtoId(), 2, n,
        header.getReceiverId(), header.getSenderId());
    rpc.send(DataPacket.fromByteArrayList(outHeader, encryptedSecrets));
    LOG.debug("Party [{}] send {}", rpc.ownParty(), outHeader);
    return null;
  }

  // step3, run on R
  List<byte[]> decryptSecrets(DataPacket packet, OTMeta meta) {
    PrivateKey key = meta.key;
    int b = meta.b;
    byte[] target = packet.getPayload().get(b);
    byte[] decryptedBytes = null;
    LOG.debug("Party [{}] decrypt secret [{}]", rpc.ownParty(), b);
    try {
      Cipher decryptCipher = Cipher.getInstance("RSA");
      decryptCipher.init(Cipher.DECRYPT_MODE, key);
      decryptedBytes = decryptCipher.doFinal(target);
    } catch (Exception e) {
      LOG.error("Error when decrypting: {}", e.getMessage());
    }
    return ImmutableList.of(decryptedBytes);
  }

  List<byte[]> senderProcedure(DataPacket initPacket) {
    LOG.debug("Party [{}] is sender of OT", rpc.ownParty());
    OTMeta meta = new OTMeta();
    DataPacketHeader expectHeader = storeSecrets(initPacket, meta);
    encryptSecrets(rpc.receive(expectHeader), meta);
    return ImmutableList.of();
  }

  List<byte[]> receiverProcedure(DataPacket initPacket) {
    LOG.debug("Party [{}] is receiver of OT", rpc.ownParty());
    OTMeta meta = new OTMeta();
    DataPacketHeader expectHeader = generateKeys(initPacket, meta);
    return decryptSecrets(rpc.receive(expectHeader), meta);
  }

  // Sender: the party who has n secrets
  boolean isSender(DataPacketHeader initHeader) {
    return initHeader.getSenderId() == rpc.ownParty().getPartyId();
  }

  @Override
  public List<byte[]> run(DataPacket initPacket) {
    DataPacketHeader header = initPacket.getHeader();
    assert header.getPtoId() == type.getId();
    if (isSender(header)) {
      return senderProcedure(initPacket);
    } else {
      return receiverProcedure(initPacket);
    }
  }

  static class OTMeta {
    int b;
    PrivateKey key;
    List<byte[]> secrets;

    OTMeta() {}

    OTMeta(int b, PrivateKey key) {
      this.b = b;
      this.key = key;
    }

    OTMeta(List<byte[]> secrets) {
      this.secrets = secrets;
    }
  }
}
