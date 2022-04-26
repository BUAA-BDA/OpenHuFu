package com.hufudb.onedb.mpc.bristol;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class BristolFileTest {

  @Test
  public void testCircuit() {
    try {
      BristolFile adder32 = CircuitType.ADD_32.getBristol();
      assertEquals(adder32.gateNum, 375);
      assertEquals(adder32.wireNum, 439);
      assertEquals(adder32.in1, 32);
      assertEquals(adder32.in2, 32);
      assertEquals(adder32.out, 33);
      assertEquals(adder32.gates.size(), adder32.gateNum);
      BristolFile adder64 = CircuitType.ADD_64.getBristol();
      assertEquals(adder64.gateNum, 759);
      assertEquals(adder64.wireNum, 887);
      assertEquals(adder64.in1, 64);
      assertEquals(adder64.in2, 64);
      assertEquals(adder64.out, 65);
      assertEquals(adder64.gates.size(), adder64.gateNum);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
