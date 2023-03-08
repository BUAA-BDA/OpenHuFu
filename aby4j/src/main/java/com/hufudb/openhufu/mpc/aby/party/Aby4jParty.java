package com.hufudb.openhufu.mpc.aby.party;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.mpc.codec.OpenHuFuCodec;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;

public class Aby4jParty {
  final int pid;
  final String address;
  final int port;
  final Party party;

  public Aby4jParty(int partyId, String address, int port) {
    this.pid = partyId;
    this.address = address;
    this.port = port;
    this.party = new Party(partyId, address, port);
  }

  public boolean addClient(int partyId, String address, int port) {
    return party.AddClient(partyId, address, port);
  }

  public void reset(int partyId) {
    party.Reset(partyId);
  }

  List<byte[]> greater(ColumnType type, e_role role, int pid, List<byte[]> inputs) {
    switch(type) {
      case BYTE:
      case SHORT:
      case INT:
        boolean result = party.GreaterI32(role, pid, OpenHuFuCodec.decodeInt(inputs.get(0)));
        return ImmutableList.of(OpenHuFuCodec.encodeBoolean(result));
      default:
        throw new UnsupportedOperationException("Unsupport type for aby");
    }
  }

  public List<byte[]> runProtocol(OperatorType op, ColumnType type, e_role role, int pid, List<byte[]> inputs) {
    switch(op) {
      case GT:
        return greater(type, role, pid, inputs);
      default:
        throw new UnsupportedOperationException("Unsupport operation for aby");
    }
  }
}
