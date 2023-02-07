package com.hufudb.openhufu.mpc.elgamal;

import com.hufudb.openhufu.mpc.codec.OpenHuFuCodec;
import java.security.SecureRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class Elgamal {
  private static final Logger LOG = LoggerFactory.getLogger(Elgamal.class);
  private final static int KEY_LENGTH = 1024;
  private final static int BYTE_LENGTH = 8;
  private final static String P = "126535388605402049049542973700958236539328315496910854523023592117026468516051843151824028176566406798134663925084909684138624303619004726680611032556927207945968721000140659332734488248277781125853940159849610018715048962115920834878804577144091376289374425091008279570513505570480918437693088952695686351367";
  private final static String G = "81248327123767948489976358691257319856321336397871450007082941210606545541618675883159315182973073472812273343241856124199389453023888448799933381897136941917208747028694565238655230858624623713754823869315162707332688928226163297833184607020661600543366919364959232770876947639319489546634455970916393673686";
  private final static SecureRandom rnd = new SecureRandom();
  private final BigInteger p;
  private final BigInteger g;
  private final BigInteger h;
  private BigInteger x;

  public static Elgamal create(boolean usePreGenPG) {
    BigInteger p;
    BigInteger g;
    if (usePreGenPG) {
      p = new BigInteger(P);
      g = new BigInteger(G);
    } else {
      BigInteger q = BigInteger.probablePrime(KEY_LENGTH - 1, rnd);
      p = q.multiply(BigInteger.TWO).add(BigInteger.ONE);
      while (!p.isProbablePrime(100) || p.bitLength() != KEY_LENGTH) {
        q = BigInteger.probablePrime(KEY_LENGTH - 1, rnd);
        p = q.multiply(BigInteger.TWO).add(BigInteger.ONE);
      }
      g = new BigInteger(KEY_LENGTH, rnd);
      while (g.compareTo(p) >= 0) g = new BigInteger(KEY_LENGTH, rnd);
      BigInteger gPow2 = g.modPow(BigInteger.TWO, p);
      BigInteger gPowQ = g.modPow(q, p);
      while (g.compareTo(BigInteger.ZERO) == 0 || gPow2.compareTo(BigInteger.ONE) == 0 || gPowQ.compareTo(BigInteger.ONE) == 0) {
        g = new BigInteger(KEY_LENGTH, rnd);
        while (g.compareTo(p) >= 0) g = new BigInteger(KEY_LENGTH, rnd);
        gPow2 = g.modPow(BigInteger.TWO, p);
        gPowQ = g.modPow(q, p);
      }
      LOG.debug("generate p, g successfully");
    }
    return new Elgamal(p, g);
  }

  public static Elgamal create(byte[] pByteArray, byte[] gByteArray, byte[] publicKey) {
    return new Elgamal(new BigInteger(pByteArray), new BigInteger(gByteArray), new BigInteger(publicKey));
  }

  private Elgamal(BigInteger p, BigInteger g) {
    this.p = p;
    this.g = g;
    x = new BigInteger(KEY_LENGTH, rnd);
    while (x.compareTo(p) >= 0 || x.compareTo(BigInteger.ZERO) == 0) x = new BigInteger(KEY_LENGTH, rnd);
    h = g.modPow(x, p);
  }

  private Elgamal(BigInteger p, BigInteger g, BigInteger h) {
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
    List<BigInteger> plainTextList = OpenHuFuCodec.decodeBigInteger(plainText, KEY_LENGTH / BYTE_LENGTH - 1, (byte) 1);
    List<BigInteger> c1List = new ArrayList<>();
    List<BigInteger> c2List = new ArrayList<>();
    for (BigInteger bigInteger : plainTextList) {
      ElgamalCipher cipher = encrypt(bigInteger);
      c1List.add(cipher.c1);
      c2List.add(cipher.c2);
    }
    c1List.addAll(c2List);
    return OpenHuFuCodec.encodeBigInteger(c1List, KEY_LENGTH / BYTE_LENGTH);
  }

  public byte[] decrypt(byte[] cipherText) {
    List<BigInteger> cipherTextList = OpenHuFuCodec.decodeBigInteger(cipherText, KEY_LENGTH / BYTE_LENGTH, (byte) 0);
    assert cipherTextList.size() % 2 == 0;
    int len = cipherTextList.size() / 2;
    List<BigInteger> plainTextList = new ArrayList<>();
    for (int i = 0; i < len; i++) {
      ElgamalCipher cipher = new ElgamalCipher(cipherTextList.get(i), cipherTextList.get(i + len));
      plainTextList.add(decrypt(cipher));
    }
    return OpenHuFuCodec.encodeOriginalData(plainTextList, KEY_LENGTH / BYTE_LENGTH - 1);
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
