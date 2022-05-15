package com.hufudb.onedb.core.sql.expression;

import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.SearchList;
import com.hufudb.onedb.core.data.TypeConverter;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import org.apache.calcite.rex.RexLiteral;

/*
 * leaf node of expression tree
 */
public class OneDBLiteral implements OneDBExpression {
  ColumnType type;
  Object value;

  OneDBLiteral(ColumnType type, Object value) {
    this.type = type;
    this.value = value;
  }

  public static OneDBExpression fromLiteral(RexLiteral literal) {
    ColumnType type = TypeConverter.convert2OneDBType(literal.getTypeName());
    Object value = literal.getValue2();
    if (type == ColumnType.SARG) {
      SearchList searchList = new SearchList(
              TypeConverter.convert2OneDBType(literal.getType().getSqlTypeName()), value);
      return new OneDBLiteral(type, searchList);
    }
    return new OneDBLiteral(type, value);
  }

  public static OneDBExpression fromProto(ExpressionProto proto) {
    ColumnType type = ColumnType.of(proto.getOutType());
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
      case SARG:
        value = SearchList.fromProto(proto);
        break;
      default:
        throw new RuntimeException("can't translate " + proto);
    }
    return new OneDBLiteral(type, value);
  }

  @Override
  public ExpressionProto toProto() {
    ExpressionProto.Builder builder =
            ExpressionProto.newBuilder()
                    .setOpType(OneDBOpType.LITERAL.ordinal())
                    .setOutType(type.ordinal())
                    .setLevel(Level.PUBLIC.getId());
    switch (type) {
      case BOOLEAN:
        return builder.setB((Boolean) value).build();
      case BYTE:
      case SHORT:
      case INT:
        return builder.setI32(((Number) value).intValue()).build();
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        return builder.setI64(((Number) value).longValue()).build();
      case FLOAT:
        return builder.setF32(((Number) value).floatValue()).build();
      case DOUBLE:
        return builder.setF64(((Number) value).doubleValue()).build();
      case STRING:
        return builder.setStr((String) value).build();
      case SARG:
        return ((SearchList) value).toProto();
      default:
        throw new RuntimeException("can't translate " + type);
    }
  }

  @Override
  public ColumnType getOutType() {
    return type;
  }

  @Override
  public OneDBOpType getOpType() {
    return OneDBOpType.LITERAL;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public Level getLevel() {
    return Level.PUBLIC;
  }
}
