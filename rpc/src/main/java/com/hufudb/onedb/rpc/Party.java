package com.hufudb.onedb.rpc;

public interface Party extends Comparable<Party> {
  int getPartyId();

  String getPartyName();

  @Override
  default int compareTo(Party otherPartySpec) {
    return Integer.compare(getPartyId(), otherPartySpec.getPartyId());
  }
}
