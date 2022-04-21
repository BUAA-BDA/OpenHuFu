package com.hufudb.onedb.rpc.grpc;

import com.hufudb.onedb.rpc.Party;

public class OneDBOwnerInfo implements Party {
  private final int id;
  private final String endpoint;

  public OneDBOwnerInfo(int id, String endpoint) {
    this.id = id;
    this.endpoint = endpoint;
  }

  @Override
  public int getPartyId() {
    return id;
  }

  @Override
  public String getPartyName() {
    return endpoint;
  }

  @Override
  public String toString() {
    return String.format("Owner[%d](%s)", id, endpoint);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof OneDBOwnerInfo)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    OneDBOwnerInfo that = (OneDBOwnerInfo) obj;
    return this.id == that.id && this.endpoint.equals(that.endpoint);
  }
}
