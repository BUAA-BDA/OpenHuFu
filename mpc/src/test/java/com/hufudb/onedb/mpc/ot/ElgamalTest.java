package com.hufudb.onedb.mpc.ot;

import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;

import static org.junit.Assert.*;

public class ElgamalTest {

  @Before
  public void setUp() throws Exception {
    Elgamal.setPG();
  }

  @Test
  public void testEncrypt() {
    Elgamal elgamal = new Elgamal();
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