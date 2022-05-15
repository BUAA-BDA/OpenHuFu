package com.hufudb.onedb.expression;

import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;

public class ExpressionFactory {
  private ExpressionFactory() {}

  public static Expression createInputRef(int ref, ColumnType type, Modifier modifier) {
    return Expression.newBuilder().setOpType(OperatorType.REF).setI32(ref).setOutType(type).setModifier(modifier).build();
  }

  public static List<Expression> createInputRef(List<Expression> inputs) {
    ImmutableList.Builder<Expression> refs = ImmutableList.builder();
    for (int i = 0; i < inputs.size(); ++i) {
      refs.add(createInputRef(i, inputs.get(i).getOutType(), inputs.get(i).getModifier()));
    }
    return refs.build();
  }

  public static List<Expression> createInputRef(Schema inputSchema) {
    ImmutableList.Builder<Expression> refs = ImmutableList.builder();
    for (int i = 0; i < inputSchema.size(); ++i) {
      refs.add(createInputRef(i, inputSchema.getType(i), inputSchema.getModifier(i)));
    }
    return refs.build();
  }

  public static Expression createBinaryOperator(OperatorType opType, ColumnType type,
      Expression left, Expression right) {
    return Expression.newBuilder().setOpType(opType).setOutType(type).addIn(left).addIn(right)
        .setModifier(ModifierWrapper.dominate(left.getModifier(), right.getModifier())).build();
  }

  public static Expression createBinaryOperator(OperatorType opType, ColumnType type,
      Expression in) {
    return Expression.newBuilder().setOpType(opType).setOutType(type).addIn(in)
        .setModifier(in.getModifier()).build();
  }

  public static Expression createLiteral(ColumnType type, Object value) {
    Expression.Builder builder = Expression.newBuilder().setOpType(OperatorType.LITERAL).setOutType(type).setModifier(Modifier.PUBLIC);
    switch (type) {
      case BOOLEAN:
        return builder.setB((Boolean) value).build();
      case STRING:
        return builder.setStr((String) value).build();
      case FLOAT:
        return builder.setF32(((Number) value).floatValue()).build();
      case DOUBLE:
        return builder.setF64(((Number) value).doubleValue()).build();
      case BYTE:
      case SHORT:
      case INT:
        return builder.setI32(((Number) value).intValue()).build();
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        return builder.setI64(((Number) value).longValue()).build();
      default:
        throw new UnsupportedOperationException("Unsupported literal type");
    }
  }
}
