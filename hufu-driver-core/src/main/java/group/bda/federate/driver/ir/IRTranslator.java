package group.bda.federate.driver.ir;

import java.util.ArrayList;
import java.util.List;


import group.bda.federate.data.Header;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.rpc.FederateCommon.Func;
import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.rpc.FederateCommon.IRField;
import group.bda.federate.rpc.FederateCommon.LiteralField;
import group.bda.federate.rpc.FederateCommon.RowField;
import group.bda.federate.rpc.FederateCommon.Op;
import group.bda.federate.sql.type.FederateFieldType;
import group.bda.federate.sql.type.Point;

public class IRTranslator {
  protected Header tableHeader;
  protected List<IR> irs;
  protected int size;

  protected static class ExpressionContext {
    Header tableHeader;
    List<IR> irs;
    int size;

    ExpressionContext(Header tableHeader, List<IR> irs) {
      this.tableHeader = tableHeader;
      this.irs = irs;
      this.size = tableHeader.size();
    }
  }

  public IRTranslator(Expression e, Header tableHeader) {
    this.tableHeader = tableHeader;
    this.irs = e.getIrList();
    this.size = tableHeader.size();
  }

  public String translate() {
    if (irs.size() > 0) {
      return getIRString(irs.get(irs.size() - 1));
    }
    return "";
  }

  private String getColumn(IR ir) {
    IRField left = ir.getInList().get(0);
    IRField right = ir.getInList().get(1);
    if (left.hasRef() && left.getRef() < size) {
      int ref = left.getRef();
      return getInputRef(ref);
    } else if (left.hasRef() && left.getRef() >= size) {
      int ref = irs.get(left.getRef() - size).getIn(0).getRef();
      return getInputRef(ref);
    } else if (right.hasRef() && right.getRef() < size) {
      int ref = right.getRef();
      return getInputRef(ref);
    } else if (right.hasRef() && right.getRef() >= size) {
      int ref = irs.get(right.getRef() - size).getIn(0).getRef();
      return getInputRef(ref);
    } else {
      return "";
    }
  }

  public Point getPoint(IR ir) {
    IRField left = ir.getInList().get(0);
    IRField right = ir.getInList().get(1);
    LiteralField p;
    if (!right.hasRef()) {
      p = right.getLiteral();
    } else if (!left.hasRef()) {
      p = left.getLiteral();
    } else {
      return null;
    }
    double x = p.getValue().getP().getLongitude();
    double y = p.getValue().getP().getLatitude();
    return new Point(x, y);
  }

  public IR getDWithin() {
    for (IR ir : irs) {
      if (ir.getOp() == Op.kScalarFunc && ir.getFunc() == Func.kDWithin) {
        return ir;
      }
    }
    return null;
  }

  public double getDisOfDWithin(IR dwithin) {
    LiteralField liter = dwithin.getInList().get(2).getLiteral();
    if (FederateFieldType.values()[liter.getType()].equals(FederateFieldType.DOUBLE)) {
      return liter.getValue().getF64();
    } else {
      return  liter.getValue().getI32();
    }
  }

  public String ColumnOfDWithin(IR dwithin) {
    return getColumn(dwithin);
  }

  public Point getPointOfDWithin(IR dwithin) {
    return getPoint(dwithin);
  }

  public IR getKNN() {
    for (IR ir : irs) {
      if (ir.getOp() == Op.kScalarFunc && ir.getFunc() == Func.kKNN) {
        return ir;
      }
    }
    return null;
  }

  public int getKOfkNN(IR kNN) {
    LiteralField liter = kNN.getInList().get(2).getLiteral();
    if (FederateFieldType.values()[liter.getType()].equals(FederateFieldType.LONG)) {
      return (int)liter.getValue().getI64();
    } else {
      return liter.getValue().getI32();
    }
  }

  public String getColumnOfkNN(IR kNN) {
    return getColumn(kNN);
  }

  public Point getPointOfkNN(IR kNN) {
    return getPoint(kNN);
  }

  protected String getIRField(IRField field) {
    if (field.hasRef()) {
      int ref = field.getRef();
      if (ref < size) {
        return getInputRef(ref);
      } else {
        return getIRString(irs.get(ref - size));
      }
    } else {
      return getLiteral(field.getLiteral());
    }
  }

  protected String getIRString(IR ir) {
    int inputSize = ir.getInCount();
    switch (ir.getOp()) {
      case kScalarFunc:
        return getScalarFuncString(ir);
      case kAggFunc:
        return getAggFuncString(ir);
      default:
        if (inputSize == 1) {
          return getUnaryString(ir);
        } else if (inputSize == 2) {
          return getBinaryIRString(ir);
        } else {
          throw new RuntimeException("can't translate ir " + ir);
        }
    }
  }

  protected String getScalarFuncString(IR ir) {
    Func func = ir.getFunc();
    List<String> inputs = new ArrayList<>();
    for (IRField field : ir.getInList()) {
      inputs.add(getIRField(field));
    }
    switch (func) {
      case kDWithin:
        if (inputs.size() != 3) {
          throw new RuntimeException("DWithin need 3 arguments, but give " + inputs.size());
        }
        return String.format("ST_DWithin(%s, %s, %s)", inputs.get(0), inputs.get(1), inputs.get(2));
      case kDistance:
        if (inputs.size() != 2) {
          throw new RuntimeException("Distance need 2 arguments, but give " + inputs.size());
        }
        return String.format("%s <-> %s", inputs.get(0), inputs.get(1));
      case kPoint:
        if (inputs.size() != 2) {
          throw new RuntimeException("Point need 2 arguments, but give " + inputs.size());
        }
        return String.format("'SRID=4326;POINT(%s %s)'", inputs.get(0), inputs.get(1));
      case kKNN:
        if (inputs.size() != 3) {
          throw new RuntimeException("KNN need 3 arguments, but give " + inputs.size());
        }
        return "TRUE";
      default:
        throw new RuntimeException("can't translate scalarFunc " + ir);
    }
  }

  protected String getAggFuncString(IR ir) {
    Func func = ir.getFunc();
    List<String> inputs = new ArrayList<>();
    for (IRField field : ir.getInList()) {
      inputs.add(getIRField(field));
    }
    switch (func) {
      case kCount:
        if (inputs.size() > 1) {
          throw new RuntimeException("COUNT need 0 or 1 arguments, but give " + inputs.size());
        } else if (inputs.size() == 1) {
          return String.format("COUNT(%s)", inputs.get(0));
        } else {
          return String.format("COUNT(*)");
        }
      case kSum:
        if (inputs.size() != 1) {
          throw new RuntimeException("SUM need 1 arguments, but give " + inputs.size());
        }
        return String.format("SUM(%s)", inputs.get(0));
      case kAvg:
        if (inputs.size() != 1) {
          throw new RuntimeException("AVG need 1 arguments, but give " + inputs.size());
        }
        return String.format("AVG(%s)", inputs.get(0));
      case kMax:
        if (inputs.size() != 1) {
          throw new RuntimeException("MAX need 1 arguments, but give " + inputs.size());
        }
        return String.format("MAX(%s)", inputs.get(0));
      case kMin:
        if (inputs.size() != 1) {
          throw new RuntimeException("MIN need 1 arguments, but give " + inputs.size());
        }
        return String.format("MIN(%s)", inputs.get(0));
      default:
        throw new RuntimeException("can't translate scalarFunc " + ir);
    }
  }

  protected String getUnaryString(IR ir) {
    String in = getIRField(ir.getIn(0));
    switch (ir.getOp()) {
      case kAs:
        return String.format("%s", in);
      case kNot:
        return String.format("(NOT %s)", in);
      case kPlus:
        return String.format("(+%s)", in);
      case kMinus:
        return String.format("(-%s)", in);
      default:
        throw new RuntimeException("can't translate unary " + ir);
    }
  }

  protected String getBinaryIRString(IR ir) {
    String left = getIRField(ir.getIn(0));
    String right = getIRField(ir.getIn(1));
    String op = "";
    switch (ir.getOp()) {
      case kGt:
        op = ">";
        break;
      case kGe:
        op = ">=";
        break;
      case kLt:
        op = "<";
        break;
      case kLe:
        op = "<=";
        break;
      case kEq:
        op = "=";
        break;
      case kNe:
        op = "<>";
        break;
      case kPlus:
        op = "+";
        break;
      case kMinus:
        op = "-";
        break;
      case kTimes:
        op = "*";
        break;
      case kDivide:
        op = "/";
        break;
      case kMod:
        op = "%";
        break;
      case kAnd:
        op = "AND";
        break;
      case kOr:
        op = "OR";
        break;
      default:
        throw new RuntimeException("can't translate binary " + ir);
    }
    return String.format("(%s %s %s)", left, op, right);
  }

  protected String getInputRef(int ref) {
    return tableHeader.getName(ref);
  }

  protected String getLiteral(LiteralField literal) {
    FederateFieldType type = FederateFieldType.values()[literal.getType()];
    RowField value = literal.getValue();
    switch (type) {
      case BOOLEAN:
        return String.valueOf(value.getB());
      case BYTE:
      case SHORT:
      case INT:
        return String.valueOf(value.getI32());
      case LONG:
        // todo: deal with time type
      case DATE:
      case TIME:
      case TIMESTAMP:
        return String.valueOf(value.getI64());
      case FLOAT:
        return String.valueOf(value.getF32());
      case DOUBLE:
        return String.valueOf(value.getF64());
      case POINT:
        return String.format("'SRID=4326;POINT(%f %f)'", value.getP().getLongitude(), value.getP().getLatitude());
      default:
        throw new RuntimeException("can't translate literal " + literal);
    }
  }
}
