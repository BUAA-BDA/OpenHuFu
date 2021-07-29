package group.bda.federate.driver.utils;

import group.bda.federate.data.Header;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.driver.ir.IRTranslator;

public class SimbaIRTranslator extends IRTranslator{
  public SimbaIRTranslator(Expression e, Header tableHeader) {
    super(e, tableHeader);
  }
}