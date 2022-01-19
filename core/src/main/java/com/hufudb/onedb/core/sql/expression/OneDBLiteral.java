package com.hufudb.onedb.core.sql.expression;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.TypeConverter;

import org.apache.calcite.rex.RexLiteral;

import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;

/*
* leaf node of expression tree
*/
public class OneDBLiteral implements OneDBExpression {
  FieldType type;
  Object value;

  OneDBLiteral(FieldType type, Object value) {
    this.type = type;
    this.value = value;
  }

  public static OneDBExpression fromLiteral(RexLiteral literal) {
    FieldType type = TypeConverter.convert2OneDBType(literal.getTypeName());
    Object value = literal.getValue2();
    return new OneDBLiteral(type, value);
  }


  public static OneDBExpression fromProto(ExpressionProto proto) {
    FieldType type = FieldType.of(proto.getOutType());
    Object value;
    switch (type) {
    case BOOLEAN:
      value = proto.getB();
      break;
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
    return new OneDBLiteral(type, value);
  }

  @Override
  public ExpressionProto toProto() {
    ExpressionProto.Builder builder = ExpressionProto.newBuilder().setOpType(OneDBOpType.LITERAL.ordinal())
        .setOutType(type.ordinal());
    switch (type) {
    case BOOLEAN:
      return builder.setB((Boolean) value).build();
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

  @Override
  public FieldType getOutType() {
    return type;
  }

  @Override
  public OneDBOpType getOpType() {
    return OneDBOpType.LITERAL;
  }

  public Object getValue() {
    return value;
  }
}
