package com.hufudb.onedb.mpc.bristol;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BristolFile {
  static final Logger LOG = LoggerFactory.getLogger(BristolFile.class);

  final int gateNum;
  final int wireNum;
  final int in1;
  final int in2;
  final int out;
  final Gate[] gates;
  // final ImmutableList<ImmutableList<Gate>> gates;

  private BristolFile(int gateNum, int wireNum, int in1, int in2, int out,
      Gate[] gates) {
    this.gateNum = gateNum;
    this.wireNum = wireNum;
    this.in1 = in1;
    this.in2 = in2;
    this.out = out;
    this.gates = gates;
  }

  // static ImmutableList<ImmutableList<Gate>> getConcurrentList(Gate[] gates, int[] inDegrees,
  //     List<Integer>[] record, ImmutableList<Gate> initBatch) {
  //   ImmutableList.Builder<Gate> builder = ImmutableList.builder();
  //   for (int i = 0; i < gates.length; ++i) {
  //     builder.add(gates[i]);
  //   }
  //   return ImmutableList.of(builder.build());
  //   // ImmutableList.Builder<ImmutableList<Gate>> builder = ImmutableList.builder();
  //   // ImmutableList<Gate> currentBatch = initBatch;
  //   // while (true) {
  //   //   ImmutableList.Builder<Gate> nextBatch = ImmutableList.builder();
  //   //   for (Gate gate : currentBatch) {
  //   //     List<Integer> outs = record[gate.out];
  //   //     if (outs == null) {
  //   //       continue;
  //   //     }
  //   //     for (Integer o : outs) {
  //   //       inDegrees[o]--;
  //   //       if (inDegrees[o] == 0) {
  //   //         nextBatch.add(gates[o]);
  //   //       }
  //   //     }
  //   //   }
  //   //   ImmutableList<Gate> batch = nextBatch.build();
  //   //   if (batch.size() == 0) {
  //   //     break;
  //   //   }
  //   //   currentBatch = batch;
  //   //   builder.add(batch);
  //   // }
  //   // return builder.build();
  // }

  public static BristolFile fromStream(InputStream inStream) {
    Scanner sc = new Scanner(inStream);
    int gateNum = sc.nextInt();
    int wireNum = sc.nextInt();
    int n1 = sc.nextInt();
    int n2 = sc.nextInt();
    int n3 = sc.nextInt();
    Gate[] gates = new Gate[gateNum];
    int[] inDegrees = new int[wireNum];
    List<Integer>[] record = new ArrayList[wireNum];
    for (int i = 0; i < gateNum; ++i) {
      int inNum = sc.nextInt();
      int outNum = sc.nextInt();
      if (inNum == 2) {
        int in1 = sc.nextInt();
        int in2 = sc.nextInt();
        int out = sc.nextInt();
        String type = sc.next();
        if (type.charAt(0) == 'X') {
          gates[i] = new Gate(in1, in2, out, GateType.XOR);
        } else if (type.charAt(0) == 'A') {
          gates[i] = new Gate(in1, in2, out, GateType.AND);
        } else {
          LOG.error("Unsupported Gate {} in BristolFormat File", type);
        }
        if (record[in1] == null) {
          record[in1] = new ArrayList<>();
        }
        record[in1].add(out);
        if (record[in2] == null) {
          record[in2] = new ArrayList<>();
        }
        record[in2].add(out);
        inDegrees[out] += 2;
      } else if (inNum == 1) {
        int in1 = sc.nextInt();
        int out = sc.nextInt();
        String type = sc.next();
        if (type.charAt(0) == 'I') {
          gates[i] = new Gate(in1, out, GateType.NOT);
        } else {
          LOG.error("Unsupported gate {} in BristolFormat File", type);
        }
        if (record[in1] == null) {
          record[in1] = new ArrayList<>();
        }
        record[in1].add(out);
        inDegrees[out] += 1;
      } else {
        LOG.error("Unsupported gate input number {} in BristolFormat File", inNum);
      }
    }
    sc.close();
    return new BristolFile(gateNum, wireNum, n1, n2, n3, gates);
  }

  public int getWireNum() {
    return wireNum;
  }

  public int getGateNum() {
    return gateNum;
  }

  public int getIn1() {
    return in1;
  }

  public int getIn2() {
    return in2;
  }

  public int getOut() {
    return out;
  }

  public Gate[] getGates() {
    return gates;
  }

  public static class Gate {
    public final int in1;
    public final int in2;
    public final int out;
    public final GateType type;

    public Gate(int in1, int in2, int out, GateType type) {
      this.in1 = in1;
      this.in2 = in2;
      this.out = out;
      this.type = type;
    }

    public Gate(int in1, int out, GateType type) {
      this.in1 = in1;
      this.in2 = 0;
      this.out = out;
      this.type = type;
    }

    @Override
    public String toString() {
      switch (type) {
        case AND:
        case XOR:
          return String.format("2 1 %d %d %d %s", in1, in2, out, type.getName());
        default:
          return String.format("1 1 %d %d %s", in1, out, type.getName());
      }
    }
  }
}
