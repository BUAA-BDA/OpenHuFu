package com.hufudb.onedb.mpc.ot;

import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

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

  private final Map<Long, OTCache> cache = new ConcurrentHashMap<>();

  PublicKeyOT(Rpc rpc) {
    super(rpc, ProtocolType.PK_OT);
  }

  // step 0, run on S
  DataPacketHeader storeSecrets(DataPacket packet) {
    DataPacketHeader header = packet.getHeader();
    List<byte[]> payloads = packet.getPayload();
    cache.put(header.getTaskId(), new OTCache(payloads));
    LOG.debug("Party [{}] store secrets in cache", rpc.ownParty().getPartyId());
    DataPacketHeader expect = new DataPacketHeader(header.getTaskId(), header.getPtoId(), 1,
        payloads.size(), header.getReceiverId(), header.getSenderId());
    LOG.debug("Party [{}] wait for packet {}", rpc.ownParty(), expect);
    return expect;
  }

  // step 1, run on R
  DataPacketHeader generateKeys(DataPacket packet) {
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
    cache.put(header.getTaskId(), new OTCache(b, privateKey));
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
  DataPacketHeader encryptSecrets(DataPacket packet) {
    DataPacketHeader header = packet.getHeader();
    List<byte[]> secrets = cache.get(header.getTaskId()).secrets;
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
  DataPacket decryptSecrets(DataPacket packet) {
    DataPacketHeader header = packet.getHeader();
    OTCache c = cache.get(header.getTaskId());
    PrivateKey key = c.key;
    int b = c.b;
    byte[] target = packet.getPayload().get(b);
    DataPacket res = null;
    LOG.debug("Party [{}] decrypt secret [{}]", rpc.ownParty(), b);
    try {
      Cipher decryptCipher = Cipher.getInstance("RSA");
      decryptCipher.init(Cipher.DECRYPT_MODE, key);
      byte[] decryptedBytes = decryptCipher.doFinal(target);
      res = generateResultPacket(packet, decryptedBytes);
    } catch (Exception e) {
      LOG.error("Error when decrypting: {}", e.getMessage());
    }
    return res;
  }

  DataPacket generateResultPacket(DataPacket finalPacket, byte[] decryptedBytes) {
    DataPacketHeader fHeader = finalPacket.getHeader();
    DataPacketHeader header = new DataPacketHeader(fHeader.getTaskId(), fHeader.getPtoId(), 4,
        fHeader.getSenderId(), fHeader.getReceiverId());
    return DataPacket.fromByteArrayList(header, List.of(decryptedBytes));
  }

  DataPacket senderProcedure(DataPacket initPacket) {
    LOG.debug("Party [{}] is sender of OT", rpc.ownParty());
    DataPacketHeader expectHeader = storeSecrets(initPacket);
    encryptSecrets(rpc.receive(expectHeader));
    return null;
  }

  DataPacket receiverProcedure(DataPacket initPacket) {
    LOG.debug("Party [{}] is receiver of OT", rpc.ownParty());
    DataPacketHeader expectHeader = generateKeys(initPacket);
    return decryptSecrets(rpc.receive(expectHeader));
  }

  // Sender: the party who has n secrets
  boolean isSender(DataPacketHeader initHeader) {
    return initHeader.getSenderId() == rpc.ownParty().getPartyId();
  }

  @Override
  public DataPacket run(DataPacket initPacket) {
    DataPacketHeader header = initPacket.getHeader();
    assert header.getPtoId() == type.getId();
    if (isSender(header)) {
      return senderProcedure(initPacket);
    } else {
      return receiverProcedure(initPacket);
    }
  }

  static class OTCache {
    int b;
    PrivateKey key;
    List<byte[]> secrets;

    OTCache(int b, PrivateKey key) {
      this.b = b;
      this.key = key;
    }

    OTCache(List<byte[]> secrets) {
      this.secrets = secrets;
    }
  }
}
