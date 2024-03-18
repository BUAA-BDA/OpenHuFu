package com.hufudb.openhufu.expression;

import com.hufudb.openhufu.data.storage.utils.GeometryUtils;
import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.utils.DateUtils;
import com.hufudb.openhufu.data.storage.utils.ModifierWrapper;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpressionUtils {
  private final static Logger LOG = LoggerFactory.getLogger(ExpressionUtils.class);

  /**
   * create Schema by a serie of Expressions,
   * each column in the returned Schema corresponds to an Expression.
   */
  public static Schema createSchema(List<Expression> exps) {
    Schema.Builder builder = Schema.newBuilder();
    exps.stream().forEach(exp -> {
      builder.add("", exp.getOutType(), exp.getModifier());
    });
    return builder.build();
  }

  public static List<Integer> getAggInputs(Expression agg) {
    assert agg.getOpType().equals(OperatorType.AGG_FUNC);
    return agg.getInList().stream().map(exp -> exp.getI32()).collect(Collectors.toList());
  }

  public static Object getLiteral(Expression lit) {
    switch (lit.getOutType()) {
      case BOOLEAN:
        return lit.getB();
      case VECTOR:
      case STRING:
        return lit.getStr();
      case FLOAT:
        return lit.getF32();
      case DOUBLE:
        return lit.getF64();
      case BYTE:
      case SHORT:
      case INT:
        return lit.getI32();
      case TIME:
        return DateUtils.intToTime(lit.getI32());
      case DATE:
        return DateUtils.longToDate(lit.getI64());
      case TIMESTAMP:
        return DateUtils.longToTimestamp(lit.getI64());
      case LONG:
        return lit.getI64();
      case GEOMETRY:
        return GeometryUtils.fromString(lit.getStr());
      default:
        throw new UnsupportedOperationException("Unsupported literal type");
    }
  }

  /**
   * Join multiple expressions logically with (AND) to form a single Expression
   * All conditions that need to be entered are Boolean Expression objects
   */
  public static Expression conjunctCondition(List<Expression> conditions) {
    final int size = conditions.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return conditions.get(0);
    }
    Expression base = conditions.get(0);
    for (int i = 1; i < size; ++i) {
      Expression filter = conditions.get(i);
      base = Expression.newBuilder().setOpType(OperatorType.AND).setOutType(ColumnType.BOOLEAN)
          .setModifier(ModifierWrapper.dominate(base.getModifier(), filter.getModifier()))
          .addIn(base).addIn(filter).build();
    }
    return base;
  }

  public static Expression disjunctCondtion(List<Expression> conditions) {
    final int size = conditions.size();
    if (size == 0) {
      return null;
    } else if (size == 1) {
      return conditions.get(0);
    }
    Expression base = conditions.get(0);
    for (int i = 1; i < size; ++i) {
      Expression filter = conditions.get(i);
      base = Expression.newBuilder().setOpType(OperatorType.OR).setOutType(ColumnType.BOOLEAN)
          .setModifier(ModifierWrapper.dominate(base.getModifier(), filter.getModifier()))
          .addIn(base).addIn(filter).build();
    }
    return base;
  }
}
