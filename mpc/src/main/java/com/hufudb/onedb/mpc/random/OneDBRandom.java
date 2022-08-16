package com.hufudb.onedb.mpc.random;

public interface OneDBRandom {
  int nextInt();
  int nextInt(int n);
  long nextLong();
  double nextDouble();
  float nextFloat();
  boolean nextBoolean();
  byte[] randomBytes(int size);
}
