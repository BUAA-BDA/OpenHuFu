package com.hufudb.openhufu.mpc.random;

public interface OpenHuFuRandom {
  int nextInt();
  int nextInt(int n);
  long nextLong();
  double nextDouble();
  float nextFloat();
  boolean nextBoolean();
  byte[] randomBytes(int size);
}
