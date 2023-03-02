package com.hufudb.openhufu.mpc.aby;

import com.hufudb.openhufu.mpc.ProtocolExecutor;
import com.hufudb.openhufu.mpc.ProtocolFactory;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.proto.OpenHuFuService.OwnerInfo;


public class AbyFactory implements ProtocolFactory {

  @Override
  public ProtocolExecutor create(OwnerInfo self, ProtocolType type) {
    return new Aby(self);
  }

  @Override
  public ProtocolType getType() {
    return ProtocolType.ABY;
  }
}
