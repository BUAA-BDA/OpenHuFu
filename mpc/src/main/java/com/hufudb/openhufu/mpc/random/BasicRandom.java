package com.hufudb.openhufu.mpc.random;

import java.security.SecureRandom;

public class BasicRandom implements OpenHuFuRandom {
  final SecureRandom random;

  public BasicRandom() {
    random = new SecureRandom();
  }

  @Override
  public int nextInt() {
    return random.nextInt();
  }

  @Override
  public int nextInt(int n) {
    return random.nextInt(n);
  }

  @Override
  public long nextLong() {
    return random.nextLong();
  }

  @Override
  public double nextDouble() {
    return random.nextDouble();
  }

  @Override
  public float nextFloat() {
    return random.nextFloat();
  }

  @Override
  public boolean nextBoolean() {
    return random.nextBoolean();
  }

  @Override
  public byte[] randomBytes(int size) {
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return bytes;
  }
}
