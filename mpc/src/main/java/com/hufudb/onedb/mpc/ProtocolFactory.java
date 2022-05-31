package com.hufudb.onedb.mpc;

import com.hufudb.onedb.proto.OneDBService.OwnerInfo;

public interface ProtocolFactory {
  ProtocolExecutor create(OwnerInfo info, ProtocolType type);
  ProtocolType getType();
}
