package com.hufudb.openhufu.rpc;

import java.util.Set;
import com.hufudb.openhufu.rpc.utils.DataPacket;
import com.hufudb.openhufu.rpc.utils.DataPacketHeader;

public interface Rpc {
  Party ownParty();
  Set<Party> getPartySet();
  Party getParty(int partyId);
  void connect();
  void send(DataPacket dataPacket);
  DataPacket receive(DataPacketHeader header);
  long getPayloadByteLength(boolean reset);
  long getSendDataPacketNum(boolean reset);
  void disconnect();
}
