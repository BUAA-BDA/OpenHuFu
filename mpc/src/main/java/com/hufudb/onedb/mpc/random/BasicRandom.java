package com.hufudb.onedb.mpc.random;

import java.util.Random;

public class BasicRandom implements OneDBRandom {
  final Random random;

  public BasicRandom() {
    random = new Random(System.currentTimeMillis());
  }

  @Override
  public int nextInt() {
    return random.nextInt();
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
}
