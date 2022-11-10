package com.hufudb.onedb.rpc;

import java.util.Set;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;

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
