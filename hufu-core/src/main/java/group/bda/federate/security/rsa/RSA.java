package group.bda.federate.security.rsa;

import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.Cipher;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.ArrayUtils;

/**
 * RSA utils
 */
public class RSA {
  private static final int KEY_SIZE = 1024;
  private static final int DECRYPT_BLOCK_SIZE = 128;
  private static final int ENCRYPT_BLOCK_SIZE = 117;

  public static KeyPair getKeyPair() throws NoSuchAlgorithmException {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(KEY_SIZE);
    return keyPairGenerator.generateKeyPair();
  }

  public static String byte2Base64(byte[] bytes) {
    return new String(Base64.encodeBase64(bytes));
  }

  public static byte[] base642Byte(String base64Str) {
    return Base64.decodeBase64(base64Str);
  }

  public static String getPublicKeyStr(KeyPair keyPair) {
    PublicKey pubKey = keyPair.getPublic();
    byte[] bytes = pubKey.getEncoded();
    return byte2Base64(bytes);
  }

  public static String getPrivateKeyStr(KeyPair keyPair) {
    PrivateKey priKey = keyPair.getPrivate();
    byte[] bytes = priKey.getEncoded();
    return byte2Base64(bytes);
  }

  public static PublicKey string2PublicKey(String pubKeyStr) throws Exception {
    byte[] keyBytes = base642Byte(pubKeyStr);
    X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    return keyFactory.generatePublic(keySpec);
  }

  public static PrivateKey string2PrivateKey(String priKeyStr) throws Exception {
    byte[] keyBytes = base642Byte(priKeyStr);
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    return keyFactory.generatePrivate(keySpec);
  }

  public static byte[] encrypt(byte[] content, PublicKey publicKey) throws Exception {
    Cipher cipher = Cipher.getInstance("RSA");
    cipher.init(Cipher.ENCRYPT_MODE, publicKey);
    byte[] result = new byte[0];
    for (int i = 0; i < content.length; i += ENCRYPT_BLOCK_SIZE) {
      byte[] block = cipher.doFinal(ArrayUtils.subarray(content, i, i + ENCRYPT_BLOCK_SIZE));
      result = ArrayUtils.addAll(result, block);
    }
    return Base64.encodeBase64(result);
  }

  public static String encrypt(String content, PublicKey publicKey) throws Exception {
    return new String(encrypt(content.getBytes(), publicKey));
  }

  public static byte[] decrypt(byte[] content, PrivateKey privateKey) throws Exception {
    Cipher cipher = Cipher.getInstance("RSA");
    cipher.init(Cipher.DECRYPT_MODE, privateKey);
    byte[] result = new byte[0];
    for (int i = 0; i < content.length; i += DECRYPT_BLOCK_SIZE) {
      byte[] block = cipher.doFinal(ArrayUtils.subarray(content, i, i + DECRYPT_BLOCK_SIZE));
      result = ArrayUtils.addAll(result, block);
    }
    return result;
  }

  public static String decrypt(String content, PrivateKey privateKey) throws Exception {
    return new String(decrypt(Base64.decodeBase64(content), privateKey));
  }
}
