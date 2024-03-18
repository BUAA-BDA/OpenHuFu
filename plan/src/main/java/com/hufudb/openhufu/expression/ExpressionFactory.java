package com.hufudb.openhufu.expression;

import com.hufudb.openhufu.data.storage.utils.GeometryUtils;
import java.util.Calendar;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.utils.DateUtils;
import com.hufudb.openhufu.data.storage.utils.ModifierWrapper;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinCondition;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
import org.locationtech.jts.geom.Geometry;

public class ExpressionFactory {
  private ExpressionFactory() {
  }

  public static Expression createInputRef(int ref, ColumnType type, Modifier modifier) {
    return Expression.newBuilder().setOpType(OperatorType.REF).setI32(ref).setOutType(type)
        .setModifier(modifier).build();
  }

  public static List<Expression> createInputRef(List<Expression> inputs) {
    ImmutableList.Builder<Expression> refs = ImmutableList.builder();
    for (int i = 0; i < inputs.size(); ++i) {
      refs.add(createInputRef(i, inputs.get(i).getOutType(), inputs.get(i).getModifier()));
    }
    return refs.build();
  }

  public static List<Expression> createInputRef(List<Expression> inputs, List<Integer> outputs) {
    ImmutableList.Builder<Expression> refs = ImmutableList.builder();
    for (int i = 0; i < outputs.size(); ++i) {
      Expression in = inputs.get(outputs.get(i));
      refs.add(createInputRef(outputs.get(i), in.getOutType(), in.getModifier()));
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

  public static Expression createUnaryOperator(OperatorType opType, ColumnType type,
                                               Expression in) {
    return Expression.newBuilder().setOpType(opType).setOutType(type).addIn(in)
        .setModifier(in.getModifier()).build();
  }

  public static Expression createMultiOperator(OperatorType opType, ColumnType type,
                                               List<Expression> inputs) {
    Modifier mod = ModifierWrapper.deduceModifier(inputs);
    return Expression.newBuilder().setOpType(opType).setOutType(type).addAllIn(inputs)
        .setModifier(mod).build();
  }

  public static Expression createScalarFunc(ColumnType type, String funcName,
                                            List<Expression> inputs) {
    Modifier mod = ModifierWrapper.deduceModifier(inputs);
    return Expression.newBuilder().setOpType(OperatorType.SCALAR_FUNC).setOutType(type)
        .setStr(funcName.toLowerCase()).addAllIn(inputs).setModifier(mod).build();
  }

  public static Expression createAggFunc(ColumnType type, int funcId, List<Expression> inputs) {
    Modifier mod = ModifierWrapper.deduceModifier(inputs);
    return Expression.newBuilder().setOpType(OperatorType.AGG_FUNC).setOutType(type)
        .addAllIn(inputs).setModifier(mod).setI32(funcId).build();
  }

  public static Expression createAggFunc(ColumnType type, Modifier mod, int funcId,
                                         List<Expression> inputs) {
    return Expression.newBuilder().setOpType(OperatorType.AGG_FUNC).setOutType(type)
        .addAllIn(inputs).setModifier(mod).setI32(funcId).build();
  }

  public static Expression createLiteral(ColumnType type, Object value) {
    Expression.Builder builder = Expression.newBuilder().setOpType(OperatorType.LITERAL)
        .setOutType(type).setModifier(Modifier.PUBLIC);
    switch (type) {
      case BOOLEAN:
        return builder.setB((Boolean) value).build();
      case VECTOR:
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
        return builder.setI64(((Number) value).longValue()).build();
      case DATE:
        return builder.setI64(DateUtils.calendarToDateLong((Calendar) value)).build();
      case TIME:
        return builder.setI32(DateUtils.calendarToTimeInt((Calendar) value)).build();
      case TIMESTAMP:
        return builder.setI64(DateUtils.calendarToTimestampLong((Calendar) value)).build();
      case GEOMETRY:
        return builder.setStr(GeometryUtils.toString((Geometry) value)).build();
      case INTERVAL:
        return builder.setI64(((Number) value).longValue()).build();
      default:
        throw new UnsupportedOperationException("Unsupported literal type");
    }
  }

  public static Expression createRange(Object left, Object right, ColumnType cType) {
    return null;
  }

  public static JoinCondition createJoinCondition(JoinType joinType, List<Integer> leftKeys,
                                                  List<Integer> rightKyes, Expression condition, Modifier modifier) {
    JoinCondition.Builder builder = JoinCondition.newBuilder().setType(joinType)
        .addAllLeftKey(leftKeys).addAllRightKey(rightKyes).setModifier(modifier);
    if (condition != null) {
      builder.setCondition(condition);
    }
    return builder.build();
  }
}
