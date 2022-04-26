package com.hufudb.onedb.mpc.bristol;

import java.io.InputStream;
import java.util.Scanner;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BristolFile {
  static final Logger LOG = LoggerFactory.getLogger(BristolFile.class);

  final int gateNum;
  final int wireNum;
  final int n1;
  final int n2;
  final int n3;
  final ImmutableList<Gate> gates;

  private BristolFile(int gateNum, int wireNum, int n1, int n2, int n3, ImmutableList<Gate> gates) {
    this.gateNum = gateNum;
    this.wireNum = wireNum;
    this.n1 = n1;
    this.n2 = n2;
    this.n3 = n3;
    this.gates = gates;
  }

  public static BristolFile fromStream(InputStream inStream) {
    Scanner sc = new Scanner(inStream);
    int gateNum = sc.nextInt();
    int wireNum = sc.nextInt();
    int n1 = sc.nextInt();
    int n2 = sc.nextInt();
    int n3 = sc.nextInt();
    ImmutableList.Builder<Gate> builder = ImmutableList.builderWithExpectedSize(gateNum);
    for (int i = 0; i < gateNum; ++i) {
      int inNum = sc.nextInt();
      int outNum = sc.nextInt();
      if (inNum == 2) {
        int in1 = sc.nextInt();
        int in2 = sc.nextInt();
        int out = sc.nextInt();
        String type = sc.next();
        if (type.charAt(0) == 'X') {
          builder.add(new Gate(in1, in2, out, GateType.XOR));
        } else if (type.charAt(0) == 'A') {
          builder.add(new Gate(in1, in2, out, GateType.AND));
        } else {
          LOG.error("Unsupported Gate {} in BristolFormat File", type);
        }
      } else if (inNum == 1) {
        int in1 = sc.nextInt();
        int out = sc.nextInt();
        String type = sc.next();
        if (type.charAt(0) == 'I') {
          builder.add(new Gate(in1, out, GateType.NOT));
        } else {
          LOG.error("Unsupported Gate {} in BristolFormat File", type);
        }
      } else {
        LOG.error("Unsupported gate input number {} in BristolFormat File", inNum);
      }
    }
    sc.close();
    return new BristolFile(gateNum, wireNum, n1, n2, n3, builder.build());
  }

  public static class Gate {
    int in1;
    int in2;
    int out;
    GateType type;

    public Gate(int in1, int in2, int out, GateType type) {
      this.in1 = in1;
      this.in2 = in2;
      this.out = out;
      this.type = type;
    }
  
    public Gate(int in1, int out, GateType type) {
      this.in1 = in1;
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
