package com.hufudb.openhufu.mpc.elgamal;

import org.junit.Test;

import java.security.SecureRandom;

import static org.junit.Assert.*;
import org.junit.Ignore;

public class ElgamalTest {
  @Ignore
  @Test
  public void testNotPreGenPG() {
    Elgamal elgamal = Elgamal.create(false);
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

  @Ignore
  @Test
  public void testPreGenPG() {
    Elgamal elgamal = Elgamal.create(true);
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