package com.hufudb.openhufu.core.utils;

import java.util.Iterator;

public class EmptyIterator<E> implements Iterator<E> {

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public E next() {
    return null;
  }
}
