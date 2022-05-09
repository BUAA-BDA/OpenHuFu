package com.hufudb.onedb.mpc.elgamal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Random;

public class ElgamalFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ElgamalFactory.class);
  private final static int KEY_LENGTH = 1024;
  private final static String P = "126535388605402049049542973700958236539328315496910854523023592117026468516051843151824028176566406798134663925084909684138624303619004726680611032556927207945968721000140659332734488248277781125853940159849610018715048962115920834878804577144091376289374425091008279570513505570480918437693088952695686351367";
  private final static String G = "81248327123767948489976358691257319856321336397871450007082941210606545541618675883159315182973073472812273343241856124199389453023888448799933381897136941917208747028694565238655230858624623713754823869315162707332688928226163297833184607020661600543366919364959232770876947639319489546634455970916393673686";
  private final static Random rnd = new Random();

  public static Elgamal createElgamal(boolean usePreGenPG) {
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

  public static Elgamal createElgamal(byte[] pByteArray, byte[] gByteArray, byte[] publicKey) {
    return new Elgamal(new BigInteger(pByteArray), new BigInteger(gByteArray), new BigInteger(publicKey));
  }
}
