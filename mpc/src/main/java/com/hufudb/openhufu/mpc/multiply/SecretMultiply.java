package com.hufudb.openhufu.mpc.multiply;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.mpc.RpcProtocolExecutor;
import com.hufudb.openhufu.mpc.codec.OpenHuFuCodec;
import com.hufudb.openhufu.rpc.Rpc;
import com.hufudb.openhufu.rpc.utils.DataPacket;
import com.hufudb.openhufu.rpc.utils.DataPacketHeader;

import java.util.List;

//Dan Bogdanov, Sven Laur, and Jan Willemson. 2008. Sharemind: A Framework for Fast Privacy-Preserving Computations. In ESORICS. 192–206.

public class SecretMultiply extends RpcProtocolExecutor {
  private MulCache mc;
  private List<Integer> parties;
  private long taskId;
  private long localSum;
  private boolean isLeader;
  private int idx;
  private long[][] vals;
  public SecretMultiply(Rpc rpc) {
    super(rpc, ProtocolType.SS);
  }

  private void fillVals(long u, long v) {
//    LOG.info("fillVals start in {}", ownId);
    for (int i = 0; i < parties.size(); i++) {
      if (i == idx) {
        continue;
      }
      int t = i == (idx - 1 + parties.size()) % parties.size()?
              (idx - 2 + parties.size()) % parties.size(): (idx - 1 + parties.size()) % parties.size();
      send(mc.getRan(i, true), 0 + generateStepID(t, i), parties.get(t));
      send(mc.getRan(i, false), 1 + generateStepID(i, t), parties.get(i));
    }
    for (int i = 0; i < parties.size(); i++) {
      if (i == idx) {
        continue;
      }
      vals[i][1] = u + receive(0 + generateStepID(idx, i), parties.get(getThirdEndpoint(idx, i, parties.size())));
      vals[i][0] = v + receive(1 + generateStepID(idx, i), parties.get(getThirdEndpoint(i, idx, parties.size())));
    }
    mc.setVal(vals);
  }
  private int getThirdEndpoint(int i, int j, int n) {
    return (((i + 1) % n) == j) ? (i + 2) % n : (i + 1) % n;
  }

  private int generateStepID(int i, int j) {
    return (i * parties.size() + j) * 2;
  }

  private void calShares(long u, long v) {
    LOG.info("calShares start in {}", ownId);
    for (int i = 0; i < parties.size(); i++) {
      if (i == idx) {
        continue;
      }
      send(mc.getVal(i, true), mc.getVal(i, false), 2, parties.get(i));
    }
    for (int i = 0; i < parties.size(); i++) {
      if (i == idx) {
        continue;
      }
      List<Long> res = receive2(2, parties.get(i));
      localSum += -mc.getVal(i, false) * res.get(0) + u * res.get(0) + v * res.get(1);
    }
  }

  private long sumShares() {
    LOG.info("sumShares start in {}", ownId);
    long globalSum = 0;
    if (isLeader) {
      for (int i = 0; i < parties.size(); i++) {
        if (i == idx) {
          globalSum += localSum;
        }
        else {
          globalSum += receive(3, parties.get(i));
        }
      }
    }
    else {
      send(localSum, 3, parties.get(0));
    }
    return globalSum;
  }



  private void send(long value, int stepID, int partyID) {
    LOG.info("send to {}, {}", partyID, stepID);
    DataPacketHeader header = new DataPacketHeader(taskId, getProtocolTypeId(), stepID, ownId, partyID);
    rpc.send(DataPacket.fromByteArrayList(header, ImmutableList.of(OpenHuFuCodec.encodeLong(value))));
  }

  private void send(long value1, long value2, int stepID, int partyID) {
    DataPacketHeader header = new DataPacketHeader(taskId, getProtocolTypeId(), stepID, ownId, partyID);
    rpc.send(DataPacket.fromByteArrayList(header,
            ImmutableList.of(OpenHuFuCodec.encodeLong(value1), OpenHuFuCodec.encodeLong(value2))));
  }

  private long receive(int stepID, int partyID) {
    LOG.info("receive {}, {}", partyID, stepID);
    final DataPacketHeader expect = new DataPacketHeader(taskId, getProtocolTypeId(), stepID, partyID, ownId);
    DataPacket packet = rpc.receive(expect);
    return OpenHuFuCodec.decodeLong(packet.getPayload().get(0));
//    return 0;
  }

  private List<Long> receive2(int stepID, int partyID) {
    final DataPacketHeader expect = new DataPacketHeader(taskId, getProtocolTypeId(), stepID, partyID, ownId);
    DataPacket packet = rpc.receive(expect);
    return ImmutableList.of(OpenHuFuCodec.decodeLong(packet.getPayload().get(0)),
            OpenHuFuCodec.decodeLong(packet.getPayload().get(1)));
  }

  /**
   * @param args[0] input value1
   * @param args[1] input value2
   * @return result of ColumnType for the first party, 0 for other parties
   */
  @Override
  public Object run(long taskId, List<Integer> parties, Object... args) throws ProtocolException {
    long u = (long) args[0];
    long v = (long) args[1];
    this.mc = new MulCache(parties.size());
    this.parties = parties;
    this.taskId = taskId;
    this.localSum = 0;
    this.isLeader = ownId == parties.get(0);
    this.idx = parties.indexOf(ownId);
    this.vals = new long[parties.size()][2];
    localSum += u * v + mc.ranSum(idx);
    fillVals(u, v);
    calShares(u, v);
    return sumShares();
  }
}
