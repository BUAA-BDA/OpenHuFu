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
import group.bda.federate.sql.type.FederateFieldType;

public class GeomesaIRTranslator extends IRTranslator {

  public GeomesaIRTranslator(Expression e, Header tableHeader) {
    super(e, tableHeader);
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
        return String.format("DWithin(%s, %s, %s, kilometers)", inputs.get(0), inputs.get(1), inputs.get(2));
      case kDistance:
        if (inputs.size() != 2) {
          throw new RuntimeException("Distance need 2 arguments, but give " + inputs.size());
        }
        return String.format("distance(%s, %s)", inputs.get(0), inputs.get(1));
      case kPoint:
        if (inputs.size() != 2) {
          throw new RuntimeException("Point need 2 arguments, but give " + inputs.size());
        }
        return String.format("POINT(%s %s)", inputs.get(0), inputs.get(1));
      default:
        throw new RuntimeException("can't translate scalarFunc " + ir);
    }
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
        return String.format("POINT(%f %f)", value.getP().getLongitude(), value.getP().getLatitude());
      default:
        throw new RuntimeException("can't translate literal " + literal);
    }
  }
}
