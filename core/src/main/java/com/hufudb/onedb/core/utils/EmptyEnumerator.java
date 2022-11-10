package com.hufudb.onedb.core.utils;

import org.apache.calcite.linq4j.Enumerator;

public class EmptyEnumerator<E> implements Enumerator<E> {

  @Override
  public E current() {
    return null;
  }

  @Override
  public boolean moveNext() {
    return false;
  }

  @Override
  public void reset() {
    // do nothing
  }

  @Override
  public void close() {
    // do nothing
  }
}
