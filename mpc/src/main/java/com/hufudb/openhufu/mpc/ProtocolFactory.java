package com.hufudb.openhufu.mpc;

import com.hufudb.openhufu.proto.OpenHuFuService.OwnerInfo;

public interface ProtocolFactory {
  ProtocolExecutor create(OwnerInfo info, ProtocolType type);
  ProtocolType getType();
}
