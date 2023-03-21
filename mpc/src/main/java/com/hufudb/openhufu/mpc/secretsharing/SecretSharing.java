package com.hufudb.openhufu.mpc.secretsharing;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.mpc.RpcProtocolExecutor;
import com.hufudb.openhufu.mpc.codec.OpenHuFuCodec;
import com.hufudb.openhufu.mpc.random.BasicRandom;
import com.hufudb.openhufu.mpc.random.OpenHuFuRandom;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
import com.hufudb.openhufu.rpc.Rpc;
import com.hufudb.openhufu.rpc.utils.DataPacket;
import com.hufudb.openhufu.rpc.utils.DataPacketHeader;

/**
 * (n, n) secret sharing implementation
 */
public class SecretSharing extends RpcProtocolExecutor {
  final OpenHuFuRandom random;
  private final static Lock lock = new ReentrantLock();
  private static int globalSeqNum = 0;
  private final static int SEQ_NUM_MAX = 100000;
  public SecretSharing(Rpc rpc) {
    super(rpc, ProtocolType.SS);
    this.random = new BasicRandom();
  }
  private static int newSeqNum() {
    int ans;
    lock.lock();
    try {
      ans = globalSeqNum;
      globalSeqNum = (globalSeqNum + 1) % SEQ_NUM_MAX;
    }
    finally {
      lock.unlock();
    }
    LOG.info("globalSeqNum is {}", ans);
    return ans;
  }

  List<Long> splitLong(long value, int n) {
    long sum = 0;
    ImmutableList.Builder<Long> shares = ImmutableList.builderWithExpectedSize(n);
    for (int i = 0; i < n - 1; ++i) {
      long share = random.nextLong();
      sum += share;
      shares.add(share);
    }
    shares.add(value - sum);
    return shares.build();
  }

  List<Double> splitDouble(double value, int n) {
    double sum = 0.0;
    ImmutableList.Builder<Double> shares = ImmutableList.builderWithExpectedSize(n);
    for (int i = 0; i < n - 1; ++i) {
      double share = random.nextDouble();
      sum += share;
      shares.add(share);
    }
    shares.add(value - sum);
    return shares.build();
  }

  List<? extends Number> splitSecret(ColumnType type, Object value, List<Integer> parties)
      throws ProtocolException {
    switch (type) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return splitLong(((Number) value).longValue(), parties.size());
      case FLOAT:
      case DOUBLE:
        return splitDouble(((Number) value).doubleValue(), parties.size());
      default:
        throw new ProtocolException("Unsupported data type for SecretSharing");
    }
  }

  List<? extends Number> distributeLong(long taskId, List<Integer> parties,
                                        List<? extends Number> localshares, int seqNum) {
    // send local shares to other parties
    ImmutableList.Builder<Long> shares = ImmutableList.builder();
    for (int i = 0; i < parties.size(); ++i) {
      if (parties.get(i) != ownId) {
        final DataPacketHeader header =
            new DataPacketHeader(taskId, getProtocolTypeId(), seqNum, ownId, parties.get(i));
        LOG.info("secret sharing: start sending Long from owner" + ownId + " to owner" + (i + 1) + " :" + " {}", header);
        rpc.send(DataPacket.fromByteArrayList(header,
            ImmutableList.of(OpenHuFuCodec.encodeLong(localshares.get(i).longValue()))));
        LOG.info("secret sharing: finish sending Long from owner" + ownId + " to owner" + (i + 1) + " : {}\n", header);
      }
    }
    // receive shares from other parties
    for (int i = 0; i < parties.size(); ++i) {
      if (parties.get(i) != ownId) {
        final DataPacketHeader expect =
            new DataPacketHeader(taskId, getProtocolTypeId(), seqNum, parties.get(i), ownId);
        LOG.info("secret sharing: start receiving Long from owner" + (i + 1) + " to owner" + ownId + " : {}", expect);
        DataPacket packet = rpc.receive(expect);
        shares.add(OpenHuFuCodec.decodeLong(packet.getPayload().get(0)));
        LOG.info("secret sharing: finish receiving Long from owner" + (i + 1) + " to owner" + ownId + " : {}\n", expect);
      } else {
        shares.add(localshares.get(i).longValue());
      }
    }
    return shares.build();
  }

  List<? extends Number> distributeDouble(long taskId, List<Integer> parties,
                                          List<? extends Number> localshares, int seqNum) {
    // send local shares to other parties
    ImmutableList.Builder<Double> shares = ImmutableList.builder();
    for (int i = 0; i < parties.size(); ++i) {
      if (parties.get(i) != ownId) {
        final DataPacketHeader header =
            new DataPacketHeader(taskId, getProtocolTypeId(), seqNum, ownId, parties.get(i));
        rpc.send(DataPacket.fromByteArrayList(header,
            ImmutableList.of(OpenHuFuCodec.encodeDouble(localshares.get(i).doubleValue()))));
      }
    }
    // receive shares from other parties
    for (int i = 0; i < parties.size(); ++i) {
      if (parties.get(i) != ownId) {
        final DataPacketHeader expect =
            new DataPacketHeader(taskId, getProtocolTypeId(), seqNum, parties.get(i), ownId);
        DataPacket packet = rpc.receive(expect);
        shares.add(OpenHuFuCodec.decodeDouble(packet.getPayload().get(0)));
      } else {
        shares.add(localshares.get(i).doubleValue());
      }
    }
    return shares.build();
  }

  List<? extends Number> distribute(long taskId, List<Integer> parties,
                                    List<? extends Number> shares, ColumnType type, int seqNum) {
    switch (type) {
      case FLOAT:
      case DOUBLE:
        return distributeDouble(taskId, parties, shares, seqNum);
      default:
        return distributeLong(taskId, parties, shares, seqNum);
    }
  }

  long sumLong(long taskId, List<Integer> parties, List<? extends Number> shares, int seqNum) {
    long sum = 0L;
    for (Number s : shares) {
      sum += s.longValue();
    }
    if (ownId != parties.get(0)) {
      DataPacketHeader header = new DataPacketHeader(taskId, getProtocolTypeId(), seqNum, ownId, parties.get(0));
      rpc.send(DataPacket.fromByteArrayList(header, ImmutableList.of(OpenHuFuCodec.encodeLong(sum))));
      return 0L;
    } else {
      for (int i = 1; i < parties.size(); ++i) {
        final DataPacketHeader expect = new DataPacketHeader(taskId, getProtocolTypeId(), seqNum, parties.get(i), ownId);
        DataPacket packet = rpc.receive(expect);
        sum += OpenHuFuCodec.decodeLong(packet.getPayload().get(0));
      }
      return sum;
    }
  }

  double sumDouble(long taskId, List<Integer> parties, List<? extends Number> shares, int seqNum) {
    double sum = 0;
    for (Number s : shares) {
      sum += s.doubleValue();
    }
    if (ownId != parties.get(0)) {
      DataPacketHeader header = new DataPacketHeader(taskId, getProtocolTypeId(), seqNum, ownId, parties.get(0));
      rpc.send(DataPacket.fromByteArrayList(header, ImmutableList.of(OpenHuFuCodec.encodeDouble(sum))));
      return 0L;
    } else {
      for (int i = 1; i < parties.size(); ++i) {
        final DataPacketHeader expect = new DataPacketHeader(taskId, getProtocolTypeId(), seqNum, parties.get(i), ownId);
        DataPacket packet = rpc.receive(expect);
        sum += OpenHuFuCodec.decodeDouble(packet.getPayload().get(0));
      }
      return sum;
    }
  }

  Number sum(long taskId, ColumnType type, List<Integer> parties, List<? extends Number> shares, int seqNum) {
    switch (type) {
      case FLOAT:
      case DOUBLE:
        return sumDouble(taskId, parties, shares, seqNum);
      default:
        return sumLong(taskId, parties, shares, seqNum);
    }
  }

  /**
   * @param args[0] ColumnType of the input value
   * @param args[1] input value
   * @param args[2] OperatorType
   * @return result of ColumnType for the first party, 0 for other parties
   */
  @Override
  public Object run(long taskId, List<Integer> parties, Object... args) throws ProtocolException {
    LOG.info("start to run");
    ColumnType type = (ColumnType) args[0];
    OperatorType op = (OperatorType) args[2];
    List<? extends Number> localshares = splitSecret(type, args[1], parties);
    List<? extends Number> shares = distribute(taskId, parties, localshares, type, newSeqNum());
    switch (op) {
      case PLUS:
        return sum(taskId, type, parties, shares, newSeqNum());
      default:
        throw new ProtocolException("Unsupported operation for secret sharing");
    }
  }
}
