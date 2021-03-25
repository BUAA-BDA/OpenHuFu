package tk.onedb.core.sql.expression;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import tk.onedb.core.data.FieldType;
import tk.onedb.core.data.TypeConverter;
import tk.onedb.core.sql.expression.OneDBOperator.OperatorType;
import tk.onedb.rpc.OneDBCommon.ExpressionProto;

/*
* leaf node of expression tree
*/
public class OneDBElement implements OneDBExpression {
  FieldType type;
  Object value;

  OneDBElement(FieldType type, Object value) {
    this.type = type;
    this.value = value;
  }

  public static OneDBExpression fromLiteral(RexLiteral literal) {
    FieldType type = TypeConverter.convert2OneDBType(literal.getTypeName());
    Object value = literal.getValue2();
    return new OneDBElement(type, value);
  }

  public static OneDBExpression fromInputRef(RexInputRef ref) {
    return new OneDBElement(FieldType.INPUT_REF, ref.getIndex());
  }

  public static OneDBExpression fromProto(ExpressionProto proto) {
    FieldType type = FieldType.of(proto.getOutType());
    Object value;
    switch (type) {
    case BOOLEAN:
      value = proto.getB();
      break;
    case INPUT_REF:
    case BYTE:
    case SHORT:
    case INT:
      value = proto.getI32();
      break;
    case LONG:
    case DATE:
    case TIME:
    case TIMESTAMP:
      value = proto.getI64();
      break;
    case FLOAT:
      value = proto.getF32();
      break;
    case DOUBLE:
      value = proto.getF64();
      break;
    case STRING:
      value = proto.getStr();
      break;
    default:
      throw new RuntimeException("can't translate " + proto);
    }
    return new OneDBElement(type, value);
  }

  @Override
  public ExpressionProto toProto() {
    ExpressionProto.Builder builder = ExpressionProto.newBuilder().setOpType(OperatorType.REF.ordinal())
        .setOutType(type.ordinal());
    switch (type) {
    case BOOLEAN:
      return builder.setB((Boolean) value).build();
    case INPUT_REF:
    case BYTE:
    case SHORT:
    case INT:
      return builder.setI32((Integer) value).build();
    case LONG:
    case DATE:
    case TIME:
    case TIMESTAMP:
      return builder.setI64((Long) value).build();
    case FLOAT:
      return builder.setF32((Float) value).build();
    case DOUBLE:
      return builder.setF64((Double) value).build();
    case STRING:
      return builder.setStr((String) value).build();
    default:
      throw new RuntimeException("can't translate " + type);
    }
  }
}
