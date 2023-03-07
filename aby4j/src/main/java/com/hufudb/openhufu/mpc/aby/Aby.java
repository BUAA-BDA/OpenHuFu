package com.hufudb.openhufu.mpc.aby;

import java.util.List;

import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.mpc.aby.party.Aby4jParty;
import com.hufudb.openhufu.mpc.aby.party.e_role;
import com.hufudb.openhufu.mpc.ProtocolExecutor;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
import com.hufudb.openhufu.proto.OpenHuFuService.OwnerInfo;


/**
 * Java Wrapper of ABY
 */
public class Aby implements ProtocolExecutor {
  OwnerInfo self;
  String address;
  int port;
  Aby4jParty abyParty;

  public enum Role {
    SERVER, // ALICE
    CLIENT; // BOB
  }

  static {
    // todo: load for different os
    System.loadLibrary("abyparty4j");
  }

  @Override
  public int getOwnId() {
    return self.getId();
  }



  @Override
  public ProtocolType getProtocolType() {
    return ProtocolType.ABY;
  }

  @Override
  public int getProtocolTypeId() {
    return ProtocolType.ABY.getId();
  }

  public Aby(int id, String address, int port) {
    this.self = OwnerInfo.newBuilder().setId(id).setEndpoint(String.format("%s:%d", address, port)).build();
    this.address = address;
    this.port = port;
    this.abyParty = new Aby4jParty(id, address, port);
  }

  public Aby(OwnerInfo self) {
    this.self = self;
    String[] parts = self.getEndpoint().split(":", 2);
    this.address = parts[0];
    this.port = Integer.valueOf(parts[1]);
    this.abyParty = new Aby4jParty(self.getId(), address, port);
  }

  /**
   * @param parties [Alice.id(CLIENT), Bob.id(SERVER)]
   * @param args OperatorType, ColumnType, AnotherParty.address, AnotherParty.port
   * for binary operator, e.g. >, the protocol evaluate Alice.input > Bob.input
   */

  @Override
  public Object run(long taskId, List<Integer> parties, Object... args) throws ProtocolException {
    List<byte[]> inputData = (List<byte[]>) args[0];
    OperatorType opType = (OperatorType) args[1];
    ColumnType type = (ColumnType) args[2];
    String address = (String) args[3];
    int port = (int) args[4];
    e_role role = e_role.SERVER;
    int otherId = parties.get(0);
    if (parties.get(1) != self.getId()) {
      // for client, need to connect to server
      role = e_role.CLIENT;
      otherId = parties.get(1);
      abyParty.addClient(otherId, address, port);
    }
    return abyParty.runProtocol(opType, type, role, otherId, inputData);
  }
}
