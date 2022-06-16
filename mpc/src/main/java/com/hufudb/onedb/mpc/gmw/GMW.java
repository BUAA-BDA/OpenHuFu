package com.hufudb.onedb.mpc.gmw;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolException;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.RpcProtocolExecutor;
import com.hufudb.onedb.mpc.bristol.BristolFile;
import com.hufudb.onedb.mpc.bristol.CircuitType;
import com.hufudb.onedb.mpc.bristol.BristolFile.Gate;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.ot.PublicKeyOT;
import com.hufudb.onedb.mpc.utils.BitArray;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;

/**
 * GMW implementation
 *   Participants: A and B (A < B)
 *   params:
 *     taskId, participants(2 party id), inputdata, circuit type id
 *   Step1: A and B load corresponding Bristol format file of opType and cache the file,
 *         share inputBytes to each other and cache local bytes
 *     Send DataPacket Format for A/B:
 *       Header: [ptoId: gmw, stepId: 1, senderId: A/B, recieverId: B/A, extraInfo: circuitType]
 *       Payload: [shares of inputBytes]
 *   Step2: A and B receive shares from another
 *   Step3: A and B evaluate circuits
 *     XOR/NOT are evaluated locally
 *     AND use 4-1 OT with init DataPacket  A/B:
 *       Header: [ptoId: ot, stepId: 0, senderId: A, receiverId: B, extraInfo: wireId]
 */
public class GMW extends RpcProtocolExecutor {
  final PublicKeyOT otExecutor;
  final ExecutorService threadPool;

  public GMW(Rpc rpc, PublicKeyOT otExecutor, ExecutorService service) {
    super(rpc, ProtocolType.GMW);
    this.otExecutor = otExecutor;
    this.threadPool = service;
  }

  int getOther(List<Integer> parties) {
    int ownId = rpc.ownParty().getPartyId();
    if (parties.get(0) == ownId) {
      return parties.get(1);
    } else if (parties.get(1) == ownId) {
      return parties.get(0);
    } else {
      LOG.error("{} is not participant of GMW", rpc.ownParty());
      throw new RuntimeException("Not participant of GMW");
    }
  }

  byte[] generateShares(byte[] ori, GMWMeta meta) {
    final byte[] randomMask = random.randomBytes(ori.length);
    byte[] shares = new byte[ori.length];
    OneDBCodec.xor(ori, randomMask, shares);
    final int in1 = meta.bristol.getIn1();
    final int in2 = meta.bristol.getIn2();
    if (meta.isA) {
      meta.initIn1(shares, in1);
    } else {
      meta.initIn2(shares, in2);
    }
    return randomMask;
  }

  // step 1: generate shares and send to another
  DataPacketHeader prepare(List<byte[]> inputs, GMWMeta meta) {
    byte[] shares = generateShares(inputs.get(0), meta);
    LOG.debug("{} generates shares", rpc.ownParty());
    DataPacketHeader outHeader = new DataPacketHeader(meta.taskId, getProtocolType().getId(), 1,
        (long) meta.circuitType, meta.ownId, meta.otherId);
    rpc.send(DataPacket.fromByteArrayList(outHeader, ImmutableList.of(shares)));
    LOG.debug("{} sends shares to Party [{}]", rpc.ownParty(), outHeader.getReceiverId());
    DataPacketHeader expect = new DataPacketHeader(meta.taskId, getProtocolType().getId(), 1,
        (long) meta.circuitType, meta.otherId, meta.ownId);
    LOG.debug("{} waits for shares of packet {}", rpc.ownParty(), expect);
    return expect;
  }

  // step 2: init shares from another party
  void initWire(DataPacket packet, GMWMeta meta) {
    byte[] shares = packet.getPayload().get(0);
    final int in1 = meta.bristol.getIn1();
    final int in2 = meta.bristol.getIn2();
    if (meta.isA) {
      meta.initIn2(shares, in2);
    } else {
      meta.initIn1(shares, in1);
    }
    LOG.debug("{} get shares from Party [{}]", rpc.ownParty(), packet.getHeader().getSenderId());
  }

  Callable<Boolean> evaluateAnd(GMWMeta meta, int in1, int in2, int out) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        BitArray wireSet = meta.wireSet;
        if (meta.isA) {
          boolean rb = random.nextBoolean();
          ImmutableList.Builder<byte[]> builder = ImmutableList.builder();
          builder.add(OneDBCodec
              .encodeBoolean(rb ^ ((wireSet.get(in1) ^ false) & (wireSet.get(in2) ^ false))));
          builder.add(OneDBCodec
              .encodeBoolean(rb ^ ((wireSet.get(in1) ^ false) & (wireSet.get(in2) ^ true))));
          builder.add(OneDBCodec
              .encodeBoolean(rb ^ ((wireSet.get(in1) ^ true) & (wireSet.get(in2) ^ false))));
          builder.add(OneDBCodec
              .encodeBoolean(rb ^ ((wireSet.get(in1) ^ true) & (wireSet.get(in2) ^ true))));
          otExecutor.run(meta.taskId, ImmutableList.of(meta.ownId, meta.otherId), builder.build(),
              (long) out);
          wireSet.set(out, rb);
        } else {
          int sel = (wireSet.get(in1) ? 2 : 0) + (wireSet.get(in2) ? 1 : 0);
          byte[] result = (byte[]) otExecutor.run(meta.taskId,
              ImmutableList.of(meta.otherId, meta.ownId), sel, 2, (long) out);
          wireSet.set(out, OneDBCodec.decodeBoolean(result));
        }
        return true;
      }
    };
  }

  Callable<Boolean> evaluateXor(GMWMeta meta, int in1, int in2, int out) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        meta.setBitSet(out, meta.getBitSet(in1) ^ meta.getBitSet(in2));
        return true;
      }
    };
  }

  Callable<Boolean> evaluateNot(GMWMeta meta, int in, int out) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        if (meta.isA) {
          meta.setBitSet(out, !meta.getBitSet(in));
        } else {
          meta.setBitSet(out, meta.getBitSet(in));
        }
        return true;
      }
    };
  }

  void evaluateCircuit(GMWMeta meta) {
    ImmutableList<ImmutableList<Gate>> concurrentGates = meta.bristol.getGates();
    LOG.debug("{}]starts to evaluate circuit", rpc.ownParty());
    for (ImmutableList<Gate> gates : concurrentGates) {
      List<Callable<Boolean>> concurrentTasks = new ArrayList<>();
      LOG.debug("{} evaluate batch circuit", rpc.ownParty());
      for (Gate gate : gates) {
        switch (gate.type) {
          case AND:
            concurrentTasks.add(evaluateAnd(meta, gate.in1, gate.in2, gate.out));
            break;
          case XOR:
            concurrentTasks.add(evaluateXor(meta, gate.in1, gate.in2, gate.out));
            break;
          case NOT:
            concurrentTasks.add(evaluateNot(meta, gate.in1, gate.out));
            break;
          default:
            LOG.error("Unsupported gate type {}", gate.toString());
            throw new UnsupportedOperationException("Unsupported gate type");
        }
      }
      try {
        List<Future<Boolean>> futures = threadPool.invokeAll(concurrentTasks);
        for (Future<Boolean> f : futures) {
          f.get();
        }
      } catch (Exception e) {
        LOG.error("Error when evaluate circuit in GMW: {}", e.getMessage());
        e.printStackTrace();
      }
    }
  }

  /**
   * @param args[0] List<byte[]> inputdata
   * @param args[1] int circuitId
   * @throws ProtocolException
   */
  @Override
  public Object run(long taskId, List<Integer> parties, Object... args) throws ProtocolException {
    assert parties.size() == 2;
    List<byte[]> inputData = (List<byte[]>) args[0];
    int circuitId = (int) args[1];
    CircuitType type = CircuitType.of(circuitId);
    LOG.debug("Load bristol of circuit {}", type);
    int otherId = getOther(parties);
    GMWMeta meta = new GMWMeta(type, taskId, rpc.ownParty().getPartyId(), otherId, circuitId);
    DataPacketHeader expect = prepare(inputData, meta);
    DataPacket sharesPacket = rpc.receive(expect);
    initWire(sharesPacket, meta);
    evaluateCircuit(meta);
    BitArray resultSet = meta.wireSet.get(meta.bristol.getWireNum() - meta.bristol.getOut(),
        meta.bristol.getWireNum());
    LOG.debug("Result bitset [{}]", resultSet.toString());
    return ImmutableList.of(resultSet.toByteArray());
  }

  static class GMWMeta {
    final BristolFile bristol;
    final BitArray wireSet;
    final long taskId;
    final int ownId;
    final int otherId;
    final int circuitType;
    final boolean isA;

    GMWMeta(CircuitType type, long taskId, int ownId, int otherId, int circuitType) {
      this.bristol = type.getBristol();
      this.wireSet = new BitArray(this.bristol.getWireNum());
      this.taskId = taskId;
      this.ownId = ownId;
      this.otherId = otherId;
      this.circuitType = circuitType;
      this.isA = ownId < otherId;
    }

    void initIn1(byte[] inBytes, int size) {
      BitArray in1 = BitArray.valueOf(inBytes);
      final int in1Size = bristol.getIn1();
      for (int i = 0; i < in1Size; ++i) {
        wireSet.set(i, in1.get(i));
      }
    }

    void initIn2(byte[] inBytes, int size) {
      BitArray in2 = BitArray.valueOf(inBytes);
      final int in1Size = bristol.getIn1();
      final int in2Size = bristol.getIn2();
      for (int i = 0; i < in2Size; ++i) {
        wireSet.set(i + in1Size, in2.get(i));
      }
    }

    synchronized void setBitSet(int i, boolean value) {
      wireSet.set(i, value);
    }

    synchronized boolean getBitSet(int i) {
      return wireSet.get(i);
    }
  }
}
