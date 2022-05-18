package com.hufudb.onedb.core.sql.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.hufudb.onedb.core.data.TypeConverter;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.expression.AggFuncType;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.expression.ExpressionUtils;
import com.hufudb.onedb.expression.ScalarFuncType;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Collation;
import com.hufudb.onedb.proto.OneDBPlan.Direction;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import com.hufudb.onedb.proto.OneDBPlan.JoinType;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.Sarg;

public class CalciteConverter {
  private CalciteConverter() {}

  public static Direction convert(RelFieldCollation.Direction direction) {
    switch (direction) {
      case ASCENDING:
        return Direction.ASC;
      case DESCENDING:
        return Direction.DESC;
      default:
        throw new UnsupportedOperationException("Unsupported direction for collation");
    }
  }

  public static Collation convert(RelFieldCollation coll) {
    return Collation.newBuilder().setRef(coll.getFieldIndex())
        .setDirection(convert(coll.getDirection())).build();
  }

  public static AggFuncType convert(SqlKind aggType) {
    switch (aggType) {
      case COUNT:
        return AggFuncType.COUNT;
      case AVG:
        return AggFuncType.AVG;
      case MAX:
        return AggFuncType.MAX;
      case MIN:
        return AggFuncType.MIN;
      case SUM:
        return AggFuncType.SUM;
      default:
        throw new UnsupportedOperationException("Unsupported aggregate function type");
    }
  }

  public static List<Expression> convert(List<Integer> groups, List<AggregateCall> aggs,
      List<Expression> inputs) {
    ImmutableList.Builder<Expression> builder = ImmutableList.builder();
    List<Expression> inputRefs = ExpressionFactory.createInputRef(inputs);
    for (int group : groups) {
      builder.add(ExpressionFactory.createAggFunc(inputRefs.get(group).getOutType(),
          AggFuncType.GROUPKEY.getId(), ImmutableList.of(inputRefs.get(group))));
    }
    for (AggregateCall agg : aggs) {
      boolean distinct = agg.isDistinct();
      AggFuncType funcType = convert(agg.getAggregation().getKind());
      int funcTypeId = distinct ? -funcType.getId() : funcType.getId();
      List<Expression> ins = ExpressionFactory.createInputRef(inputs, agg.getArgList());
      builder.add(ExpressionFactory.createAggFunc(
          TypeConverter.convert2OneDBType(agg.getType().getSqlTypeName()), funcTypeId, ins));
    }
    return builder.build();
  }

  public static JoinType convert(JoinRelType type) {
    switch (type) {
      case INNER:
        return JoinType.INNER;
      case LEFT:
        return JoinType.LEFT;
      case RIGHT:
        return JoinType.RIGHT;
      case FULL:
        return JoinType.OUTER;
      case SEMI:
        return JoinType.SEMI;
      default:
        throw new UnsupportedOperationException("Unsupported join type");
    }
  }

  public static JoinCondition convert(JoinRelType type, JoinInfo info, List<Expression> inputs) {
    Expression condition =
        ExpressionUtils.conjunctCondition(convert(info.nonEquiConditions, inputs));
    Modifier dominator = condition == null ? Modifier.PUBLIC : condition.getModifier();
    for (int key : info.leftKeys) {
      dominator = ModifierWrapper.dominate(dominator, inputs.get(key).getModifier());
    }
    for (int key : info.rightKeys) {
      dominator = ModifierWrapper.dominate(dominator, inputs.get(key).getModifier());
    }
    return ExpressionFactory.createJoinCondition(convert(type), info.leftKeys, info.rightKeys,
        condition, dominator);
  }

  public static List<Expression> convert(List<RexNode> exps, List<Expression> inputs) {
    return new ExpressionBuilder(exps, inputs).build();
  }

  public static List<Expression> convert(RexProgram program, List<Expression> inputs) {
    return new ExpressionBuilder(program.getExprList(), program.getProjectList(), inputs).build();
  }

  public static Expression convertLiteral(RexLiteral literal) {
    ColumnType type = TypeConverter.convert2OneDBType(literal.getTypeName());
    Object value = literal.getValue2();
    return ExpressionFactory.createLiteral(type, value);
  }


  static class ExpressionBuilder {
    List<? extends RexNode> outputs;
    List<RexNode> locals;
    List<Expression> inputs;

    ExpressionBuilder(List<RexNode> exps, List<Expression> inputs) {
      this.outputs = exps;
      this.locals = exps;
      this.inputs = inputs;
    }

    ExpressionBuilder(List<RexNode> exps, List<? extends RexNode> outputs,
        List<Expression> inputs) {
      this.outputs = outputs;
      this.locals = exps;
      this.inputs = inputs;
    }

    List<Expression> build() {
      return outputs.stream().map(node -> convert(node)).collect(Collectors.toList());
    }

    Expression convert(RexNode node) {
      switch (node.getKind()) {
        // leaf node
        case LITERAL:
          return convertLiteral((RexLiteral) node);
        case INPUT_REF:
          return inputs.get(((RexInputRef) node).getIndex());
        // binary
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case EQUALS:
        case NOT_EQUALS:
        case PLUS:
        case MINUS:
        case TIMES:
        case DIVIDE:
        case MOD:
        case AND:
        case OR:
          return binary((RexCall) node);
        // unary
        case AS:
        case NOT:
        case PLUS_PREFIX:
        case MINUS_PREFIX:
        case IS_NULL:
        case IS_NOT_NULL:
          return unary((RexCall) node);
        // case
        case CASE:
          return caseCall((RexCall) node);
        // search
        case SEARCH:
          return searchCall((RexCall) node);
        // local_ref
        case LOCAL_REF:
          return localRef((RexLocalRef) node);
        // udf
        case OTHER_FUNCTION:
          return scalarFunc((RexCall) node);
        default:
          throw new RuntimeException(String.format("not support %s", node));
      }
    }

    /**
     * add binary operator
     */
    Expression binary(RexCall call) {
      OperatorType op;
      switch (call.getKind()) {
        case GREATER_THAN:
          op = OperatorType.GT;
          break;
        case GREATER_THAN_OR_EQUAL:
          op = OperatorType.GE;
          break;
        case LESS_THAN:
          op = OperatorType.LT;
          break;
        case LESS_THAN_OR_EQUAL:
          op = OperatorType.LE;
          break;
        case EQUALS:
          op = OperatorType.EQ;
          break;
        case NOT_EQUALS:
          op = OperatorType.NE;
          break;
        case PLUS:
          op = OperatorType.PLUS;
          break;
        case MINUS:
          op = OperatorType.MINUS;
          break;
        case TIMES:
          op = OperatorType.TIMES;
          break;
        case DIVIDE:
          op = OperatorType.DIVIDE;
          break;
        case MOD:
          op = OperatorType.MOD;
          break;
        case AND:
          op = OperatorType.AND;
          break;
        case OR:
          op = OperatorType.OR;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      Expression left = convert(call.operands.get(0));
      Expression right = convert(call.operands.get(1));
      ColumnType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return ExpressionFactory.createBinaryOperator(op, type, left, right);
    }

    /*
     * add unary operator
     */
    Expression unary(RexCall call) {
      OperatorType op;
      switch (call.getKind()) {
        case AS:
          op = OperatorType.AS;
          break;
        case NOT:
          op = OperatorType.NOT;
          break;
        case PLUS_PREFIX:
          op = OperatorType.PLUS_PRE;
          break;
        case MINUS_PREFIX:
          op = OperatorType.MINUS_PRE;
          break;
        case IS_NULL:
          op = OperatorType.IS_NULL;
          break;
        case IS_NOT_NULL:
          op = OperatorType.IS_NOT_NULL;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      Expression in = convert(call.operands.get(0));
      ColumnType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return ExpressionFactory.createUnaryOperator(op, type, in);
    }

    /*
     * translate case
     */
    Expression caseCall(RexCall call) {
      // in Case Rexcall, oprands are organized as [when, then, when, then, ..., else]
      OperatorType op = OperatorType.CASE;
      List<Expression> eles =
          call.operands.stream().map(c -> convert(c)).collect(Collectors.toList());
      ColumnType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return ExpressionFactory.createMultiOperator(op, type, eles);
    }

    Expression searchCall(RexCall call) {
      Expression in = convert(call.operands.get(0));
      RexLiteral ranges = (RexLiteral) call.operands.get(1);
      return convertRangeSet((Sarg) ranges.getValue2(),
          TypeConverter.convert2OneDBType(ranges.getType().getSqlTypeName()), in);
    }

    public static Expression convertRangeSet(Sarg sarg, ColumnType type, Expression in) {
      Set<Range<Comparable>> ranges = sarg.rangeSet.asRanges();
      List<Expression> rangeExps = new ArrayList<>();
      for (Range<Comparable> r : ranges) {
        switch (type) {
          // todo: deal with single side bound scenarios
          case BYTE:
          case SHORT:
          case INT:
            rangeExps.add(convertRange(r.lowerEndpoint(), r.upperEndpoint(), r.lowerBoundType(),
                r.upperBoundType(), ColumnType.INT, in));
            break;
          case DATE:
          case TIME:
          case TIMESTAMP:
          case LONG:
            rangeExps.add(convertRange(r.lowerEndpoint(), r.upperEndpoint(), r.lowerBoundType(),
                r.upperBoundType(), ColumnType.LONG, in));
            break;
          case FLOAT:
            rangeExps.add(convertRange(r.lowerEndpoint(), r.upperEndpoint(), r.lowerBoundType(),
                r.upperBoundType(), ColumnType.FLOAT, in));
            break;
          case DOUBLE:
            rangeExps.add(convertRange(r.lowerEndpoint(), r.upperEndpoint(), r.lowerBoundType(),
                r.upperBoundType(), ColumnType.DOUBLE, in));
            break;
          case STRING:
            rangeExps.add(convertRange(r.lowerEndpoint(), r.upperEndpoint(), r.lowerBoundType(),
                r.upperBoundType(), ColumnType.STRING, in));
            break;
          default:
            throw new UnsupportedOperationException("Unsupported type for range");
        }
      }
      return ExpressionUtils.conjunctCondition(rangeExps);
    }

    public static Expression convertRange(Object left, Object right, BoundType leftBound,
        BoundType rightBound, ColumnType cType, Expression in) {
      Expression leftLit = ExpressionFactory.createLiteral(cType, left);
      Expression rightLit = ExpressionFactory.createLiteral(cType, right);
      Expression leftCmp = ExpressionFactory.createBinaryOperator(
          leftBound.equals(BoundType.CLOSED) ? OperatorType.GE : OperatorType.GT, cType, in,
          leftLit);
      Expression rightCmp = ExpressionFactory.createBinaryOperator(
          rightBound.equals(BoundType.CLOSED) ? OperatorType.LE : OperatorType.LT, cType, in,
          rightLit);
      return ExpressionFactory.createBinaryOperator(OperatorType.AND, ColumnType.BOOLEAN, leftCmp,
          rightCmp);
    }

    /*
     * translate localref
     */
    Expression localRef(RexLocalRef node) {
      RexNode local = locals.get(node.getIndex());
      // todo: this can be optimized
      return convert(local);
    }

    /*
     * translate func
     */
    Expression scalarFunc(RexCall call) {
      SqlUserDefinedFunction function = (SqlUserDefinedFunction) call.op;
      ScalarFuncType func;
      switch (function.getName()) {
        case "ABS":
          func = ScalarFuncType.ABS;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      List<Expression> eles =
          call.operands.stream().map(r -> convert(r)).collect(Collectors.toList());
      ColumnType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return ExpressionFactory.createScalarFunc(type, func.getId(), eles);
    }
  }
}
