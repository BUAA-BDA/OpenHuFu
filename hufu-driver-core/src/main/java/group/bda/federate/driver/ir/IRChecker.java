package group.bda.federate.driver.ir;

import java.util.List;

import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.rpc.FederateCommon.IRField;

public class IRChecker {
  Header tableHeader;
  Header outputHeader;
  List<Expression> exps;
  int size;

  public IRChecker(List<Expression> exps, Header tableHeader, Header outputHeader) {
    this.tableHeader = tableHeader;
    this.outputHeader = outputHeader;
    this.exps = exps;
    this.size = tableHeader.size();
  }

  public boolean check() {
    for (int i = 0; i < outputHeader.size(); ++i) {
      Expression exp = exps.get(i);
      if (exp.getLevel() == Level.HIDE.ordinal()) {
        continue;
      }
      List<IR> irs = exp.getIrList();
      if (!checkIR(irs.get(irs.size() - 1), exp.getLevel())) {
        return false;
      }
    }
    return true;
  }

  protected boolean checkIR(IR ir, int expectLevel) {
    int highestLevel = Level.PUBLIC.ordinal();
    switch (ir.getOp()) {
      case kAggFunc:
        return checkAggregate(ir, expectLevel);
      default:
        for (IRField field : ir.getInList()) {
          if (field.getLevel() > highestLevel) {
            highestLevel = field.getLevel();
          }
        }
    }
    return expectLevel == highestLevel;
  }

  protected boolean checkAggregate(IR ir, int expectLevel) {
    List<IRField> in = ir.getInList();
    int highestLevel = Level.PUBLIC.ordinal();
    for (IRField field : in) {
      if (field.getLevel() > highestLevel) {
        highestLevel = field.getLevel();
      }
    }
    int actualLevel = highestLevel;
    switch (ir.getFunc()) {
      case kCount:
        if (in.size() == 0) {
          actualLevel = outputHeader.hasPrivacy() ? Level.PROTECTED.ordinal() : Level.PUBLIC.ordinal();
          break;
        }
      case kSum:
      case kAvg:
        if (highestLevel == Level.PRIVATE.ordinal()) {
          actualLevel = Level.PROTECTED.ordinal();
          break;
        }
      case kMax:
      case kMin:
        break;
      default:
        return false;
    }
    return expectLevel == actualLevel;
  }
}
