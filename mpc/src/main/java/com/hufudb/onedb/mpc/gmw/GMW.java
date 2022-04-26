package com.hufudb.onedb.mpc.gmw;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.mpc.ProtocolExecutor;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.bristol.BristolFile;
import com.hufudb.onedb.mpc.bristol.CircuitType;
import com.hufudb.onedb.mpc.bristol.BristolFile.Gate;
import com.hufudb.onedb.mpc.codec.OneDBCodec;
import com.hufudb.onedb.mpc.ot.PublicKeyOT;
import com.hufudb.onedb.rpc.Rpc;
import com.hufudb.onedb.rpc.utils.DataPacket;
import com.hufudb.onedb.rpc.utils.DataPacketHeader;

/*-
 * GMW implementation
 *   Participants: A and B (A < B)
 *   Init DataPacket:
 *     A:
 *       Header: [ptoId: gmw, stepId: 0, senderId: A, receiverId: B, extraInfo: circuitType]
 *       DataPacket: [inputBytes]
 *     B:
 *       Header: [ptoId: gmw, stepId: 0, senderId: B, receiverId: A, extraInfo: circuitType]
 *       DataPacket: [inputBytes]
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
public class GMW extends ProtocolExecutor {
  final PublicKeyOT otExecutor;

  protected GMW(Rpc rpc, PublicKeyOT otExecutor) {
    super(rpc, ProtocolType.GMW);
    this.otExecutor = otExecutor;
  }

  boolean isA(DataPacketHeader initHeader) {
    return initHeader.getSenderId() < initHeader.getReceiverId();
  }

  byte[] generateShares(byte[] ori, GMWMeta meta) {
    final byte[] randomMask = random.randomBytes(ori.length);
    OneDBCodec.xor(ori, randomMask);
    final int in1 = meta.bristol.getIn1();
    final int in2 = meta.bristol.getIn2();
    if (meta.isA) {
      meta.initIn1(ori, in1);
    } else {
      meta.initIn2(ori, in2);
    }
    return randomMask;
  }

  // step 1: generate shares and send to another
  DataPacketHeader prepare(DataPacket initPacket, GMWMeta meta) {
    DataPacketHeader header = initPacket.getHeader();
    byte[] shares = generateShares(initPacket.getPayload().get(0), meta);
    LOG.debug("Party [{}] generates shares", rpc.ownParty());
    DataPacketHeader outHeader = new DataPacketHeader(header.getTaskId(), header.getPtoId(), 1,
        header.getExtraInfo(), header.getSenderId(), header.getReceiverId());
    rpc.send(DataPacket.fromByteArrayList(outHeader, ImmutableList.of(shares)));
    LOG.debug("Party [{}] sends shares to Party [{}]", rpc.ownParty(), outHeader.getReceiverId());
    DataPacketHeader expect = new DataPacketHeader(header.getTaskId(), header.getPtoId(), 1,
        header.getExtraInfo(), header.getReceiverId(), header.getSenderId());
    LOG.debug("Party [{}] waits for shares of packet {}", rpc.ownParty(), expect);
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
    LOG.debug("Party [{}] get shares from Party [{}]", rpc.ownParty(), packet.getHeader().getSenderId());
  }

  void evaluateAnd(GMWMeta meta, int in1, int in2, int out, boolean isA) {
    BitSet wireSet = meta.wireSet;
    DataPacketHeader initHeader = meta.initHeader;
    if (meta.isA) {
      boolean rb = random.nextBoolean();
      ImmutableList.Builder<byte[]> builder = ImmutableList.builder();
      builder.add(
          OneDBCodec.encodeBoolean(rb ^ ((wireSet.get(in1) ^ false) & (wireSet.get(in2) ^ false))));
      builder.add(
          OneDBCodec.encodeBoolean(rb ^ ((wireSet.get(in1) ^ false) & (wireSet.get(in2) ^ true))));
      builder.add(
          OneDBCodec.encodeBoolean(rb ^ ((wireSet.get(in1) ^ true) & (wireSet.get(in2) ^ false))));
      builder.add(
          OneDBCodec.encodeBoolean(rb ^ ((wireSet.get(in1) ^ true) & (wireSet.get(in2) ^ true))));
      DataPacketHeader otHeader = new DataPacketHeader(initHeader.getTaskId(),
          ProtocolType.PK_OT.getId(), 0, 4L, initHeader.getSenderId(), initHeader.getReceiverId());
      otExecutor.run(DataPacket.fromByteArrayList(otHeader, builder.build()));
      wireSet.set(out, rb);
    } else {
      int sel = (wireSet.get(in1) ? 2 : 0) + (wireSet.get(in2) ? 1 : 0);
      DataPacketHeader otHeader = new DataPacketHeader(initHeader.getTaskId(),
          ProtocolType.PK_OT.getId(), 0, 2, initHeader.getReceiverId(), initHeader.getSenderId());
      byte[] selBits = OneDBCodec.encodeInt(sel);
      List<byte[]> result =
          otExecutor.run(DataPacket.fromByteArrayList(otHeader, ImmutableList.of(selBits)));
      byte[] res = result.get(0);
      wireSet.set(out, OneDBCodec.decodeBoolean(res));
    }
  }

  void evaluateXor(BitSet wireSet, int in1, int in2, int out) {
    wireSet.set(out, wireSet.get(in1) ^ wireSet.get(in2));
  }

  void evaluateNot(BitSet wireSet, int in, int out, boolean isA) {
    if (isA) {
      wireSet.set(out, !wireSet.get(in));
    } else {
      wireSet.set(out, wireSet.get(in));
    }
  }

  void evaluateCircuit(GMWMeta meta) {
    ImmutableList<Gate> gates = meta.bristol.getGates();
    final BitSet wireSet = meta.wireSet;
    LOG.debug("Party [{}] starts to evaluate circuit", rpc.ownParty());
    // todo: optimze with concurrent
    for (Gate gate : gates) {
      switch (gate.type) {
        case AND:
          evaluateAnd(meta, gate.in1, gate.in2, gate.out, meta.isA);
          break;
        case XOR:
          evaluateXor(wireSet, gate.in1, gate.in2, gate.out);
          break;
        case NOT:
          evaluateNot(wireSet, gate.in1, gate.out, meta.isA);
          break;
        default:
          LOG.error("Unsupported gate type {}", gate.toString());
          throw new UnsupportedOperationException("Unsupported gate type");
      }
    }
  }

  @Override
  public List<byte[]> run(DataPacket initPacket) {
    DataPacketHeader header = initPacket.getHeader();
    CircuitType type = CircuitType.type((int) header.getExtraInfo());
    LOG.debug("Load bristol of circuit {}", type);
    GMWMeta meta = new GMWMeta(type, isA(header), header);
    DataPacketHeader expect = prepare(initPacket, meta);
    DataPacket sharesPacket = rpc.receive(expect);
    initWire(sharesPacket, meta);
    evaluateCircuit(meta);
    BitSet resultSet = meta.wireSet
        .get(meta.bristol.getWireNum() - meta.bristol.getOut(), meta.bristol.getWireNum());
    LOG.debug("Result bitset [{}]", resultSet.toString());
    return ImmutableList.of(resultSet.toByteArray());
  }

  static class GMWMeta {
    final BristolFile bristol;
    final BitSet wireSet;
    final boolean isA;
    final DataPacketHeader initHeader;

    GMWMeta(CircuitType type, boolean isA, DataPacketHeader initHeader) {
      this.bristol = type.getBristol();
      this.wireSet = new BitSet(this.bristol.getWireNum());
      this.isA = isA;
      this.initHeader = initHeader;
    }

    void initIn1(byte[] inBytes, int size) {
      BitSet in1 = BitSet.valueOf(inBytes);
      final int in1Size = bristol.getIn1();
      for (int i = 0; i < in1Size; ++i) {
        wireSet.set(i, in1.get(i));
      }
    }

    void initIn2(byte[] inBytes, int size) {
      BitSet in2 = BitSet.valueOf(inBytes);
      final int in1Size = bristol.getIn1();
      final int in2Size = bristol.getIn2();
      for (int i = 0; i < in2Size; ++i) {
        wireSet.set(i + in1Size, in2.get(i));
      }
    }
  }
}
