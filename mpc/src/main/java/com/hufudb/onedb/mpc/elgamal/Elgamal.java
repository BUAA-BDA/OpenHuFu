package com.hufudb.onedb.mpc.elgamal;

import com.hufudb.onedb.mpc.codec.OneDBCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Elgamal {
  private static final Logger LOG = LoggerFactory.getLogger(Elgamal.class);
  private final static int KEY_LENGTH = 1024;
  private final static int BYTE_LENGTH = 8;
  private final BigInteger p;
  private final BigInteger g;
  private final Random rnd = new Random();
  private BigInteger x;
  private final BigInteger h;


  public Elgamal(BigInteger p, BigInteger g) {
    this.p = p;
    this.g = g;
    x = new BigInteger(KEY_LENGTH, rnd);
    while (x.compareTo(p) >= 0 || x.compareTo(BigInteger.ZERO) == 0) x = new BigInteger(KEY_LENGTH, rnd);
    h = g.modPow(x, p);
  }

  public Elgamal(BigInteger p, BigInteger g, BigInteger h) {
    this.p = p;
    this.g = g;
    this.h = h;
    x = null;
  }

  public byte[] generatePseudoPublicKey() {
    assert p != null;
    BigInteger key = new BigInteger(KEY_LENGTH, rnd);
    while (key.compareTo(p) >= 0 || key.compareTo(BigInteger.ZERO) == 0) key = new BigInteger(KEY_LENGTH, rnd);
    return key.toByteArray();
  }

  public byte[] getPublicKey() {
    return h.toByteArray();
  }

  public byte[] getPByteArray() {
    return p.toByteArray();
  }

  public byte[] getGByteArray() {
    return g.toByteArray();
  }

  private ElgamalCipher encrypt(BigInteger plainText) {
    BigInteger y = new BigInteger(KEY_LENGTH, rnd);
    while (y.compareTo(p) >= 0 || y.compareTo(BigInteger.ZERO) == 0) y = new BigInteger(KEY_LENGTH, rnd);
    BigInteger c1 = g.modPow(y, p);
    BigInteger s = h.modPow(y, p);
    BigInteger c2 = plainText.multiply(s);
    c2 = c2.mod(p);
    return new ElgamalCipher(c1, c2);
  }

  private BigInteger decrypt(ElgamalCipher cipherText) {
    assert x != null;
    BigInteger s = cipherText.c1.modPow(x, p);
    BigInteger sInverse = s.modInverse(p);
    BigInteger plainText = cipherText.c2.multiply(sInverse);
    plainText = plainText.mod(p);
    return plainText;
  }

  public byte[] encrypt(byte[] plainText) {
    List<BigInteger> plainTextList = OneDBCodec.decodeBigInteger(plainText, KEY_LENGTH / BYTE_LENGTH - 1, (byte) 1);
    List<BigInteger> c1List = new ArrayList<>();
    List<BigInteger> c2List = new ArrayList<>();
    for (BigInteger bigInteger : plainTextList) {
      ElgamalCipher cipher = encrypt(bigInteger);
      c1List.add(cipher.c1);
      c2List.add(cipher.c2);
    }
    c1List.addAll(c2List);
    return OneDBCodec.encodeBigInteger(c1List, KEY_LENGTH / BYTE_LENGTH);
  }

  public byte[] decrypt(byte[] cipherText) {
    List<BigInteger> cipherTextList = OneDBCodec.decodeBigInteger(cipherText, KEY_LENGTH / BYTE_LENGTH, (byte) 0);
    assert cipherTextList.size() % 2 == 0;
    int len = cipherTextList.size() / 2;
    List<BigInteger> plainTextList = new ArrayList<>();
    for (int i = 0; i < len; i++) {
      ElgamalCipher cipher = new ElgamalCipher(cipherTextList.get(i), cipherTextList.get(i + len));
      plainTextList.add(decrypt(cipher));
    }
    return OneDBCodec.encodeOriginalData(plainTextList, KEY_LENGTH / BYTE_LENGTH - 1);
  }

  static class ElgamalCipher {
    BigInteger c1;
    BigInteger c2;

    ElgamalCipher(BigInteger c1, BigInteger c2) {
      this.c1 = c1;
      this.c2 = c2;
    }
  }
}
