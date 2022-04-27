package com.hufudb.onedb.rpc;

import java.util.Set;

public interface RpcManager {
  Rpc getRpc(int partyId);
  int getPartyNum();
  Set<Party> getPartySet();
}
