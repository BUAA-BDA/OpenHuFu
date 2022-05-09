package com.hufudb.onedb.mpc.elgamal;

import org.junit.Test;

import java.security.SecureRandom;

import static org.junit.Assert.*;

public class ElgamalTest {

  @Test
  public void testNotPreGenPG() {
    Elgamal elgamal = ElgamalFactory.createElgamal(false);
    int[] len = {1, 10, 100, 1000, 10000};
    for (int p : len) {
      byte[] text = new byte[1000];
      SecureRandom random = new SecureRandom();
      random.nextBytes(text);
      byte[] cipher = elgamal.encrypt(text);
      byte[] plain = elgamal.decrypt(cipher);
      assertArrayEquals(text, plain);
    }
  }

  @Test
  public void testPreGenPG() {
    Elgamal elgamal = ElgamalFactory.createElgamal(true);
    int[] len = {1, 10, 100, 1000, 10000};
    for (int p : len) {
      byte[] text = new byte[1000];
      SecureRandom random = new SecureRandom();
      random.nextBytes(text);
      byte[] cipher = elgamal.encrypt(text);
      byte[] plain = elgamal.decrypt(cipher);
      assertArrayEquals(text, plain);
    }
  }
}