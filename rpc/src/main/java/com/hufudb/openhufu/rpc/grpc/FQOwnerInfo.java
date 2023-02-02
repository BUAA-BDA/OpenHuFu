package com.hufudb.openhufu.rpc.grpc;

import com.hufudb.openhufu.rpc.Party;
import com.hufudb.openhufu.proto.OpenHuFuService.OwnerInfo;

public class FQOwnerInfo implements Party {
  private final int id;
  private final String endpoint;

  public FQOwnerInfo(int id, String endpoint) {
    this.id = id;
    this.endpoint = endpoint;
  }

  public static FQOwnerInfo fromProto(OwnerInfo proto) {
    return new FQOwnerInfo(proto.getId(), proto.getEndpoint());
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
    if (!(obj instanceof FQOwnerInfo)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    FQOwnerInfo that = (FQOwnerInfo) obj;
    return this.id == that.id && this.endpoint.equals(that.endpoint);
  }
}
