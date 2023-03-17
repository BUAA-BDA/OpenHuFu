package com.hufudb.openhufu.core.sql.expression;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.hufudb.openhufu.core.data.TypeConverter;
import com.hufudb.openhufu.data.storage.utils.ModifierWrapper;
import com.hufudb.openhufu.expression.AggFuncType;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.expression.ExpressionUtils;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Collation;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Direction;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinCondition;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
import com.hufudb.openhufu.udf.UDFLoader;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalciteConverter {
  private static final Logger LOG = LoggerFactory.getLogger(CalciteConverter.class);

  private CalciteConverter() {
  }

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
        LOG.error("{} is unsupported by openhufu", aggType);
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
      // distinct aggregation has a AggFuncType reversed
      int funcTypeId = distinct ? -funcType.getId() : funcType.getId();
      // AggregateCall.argList is empty means using (*)
      List<Expression> ins = agg.getArgList().isEmpty() ?
          ImmutableList.of() : ExpressionFactory.createInputRef(inputs, agg.getArgList());
      Modifier modifier = ModifierWrapper.deduceModifier(inputRefs);
      // if agg on private col(s) is allowed, the col is then protected instead of still private
      if (AggFuncType.isAllowedOnPrivate(funcType.getId())) {
        modifier = modifier.equals(Modifier.PRIVATE) ? Modifier.PROTECTED : modifier;
      }
      builder.add(ExpressionFactory.createAggFunc(
          TypeConverter.convert2OpenHuFuType(agg.getType().getSqlTypeName()),
          modifier, funcTypeId, ins));
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
    ColumnType type = TypeConverter.convert2OpenHuFuType(literal.getTypeName());
    switch (type) {
      // NOTE:  rely on calcite unstable api in RexLiteral.java, check this when calcite update
      case DATE:
      case TIME:
      case TIMESTAMP:
        return ExpressionFactory.createLiteral(type, literal.getValueAs(Calendar.class));
      case INTERVAL:
        return ExpressionFactory.createLiteral(type, literal.getValueAs(Long.class));
      default:
        return ExpressionFactory.createLiteral(type, literal.getValue2());
    }
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
        case LIKE:
          return binary((RexCall) node);
        // unary
        case AS:
        case NOT:
        case PLUS_PREFIX:
        case MINUS_PREFIX:
        case IS_NULL:
        case IS_NOT_NULL:
        case CAST: // todo support cast
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
        case LIKE:
          op = OperatorType.LIKE;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      Expression left = convert(call.operands.get(0));
      Expression right = convert(call.operands.get(1));
      ColumnType type = TypeConverter.convert2OpenHuFuType(call.getType().getSqlTypeName());
      return ExpressionFactory.createBinaryOperator(op, type, left, right);
    }

    /**
     * add unary operator
     */
    Expression unary(RexCall call) {
      OperatorType op;
      Expression in = convert(call.operands.get(0));
      switch (call.getKind()) {
        case AS:
        case CAST: // todo: support cast
          return in;
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
      ColumnType type = TypeConverter.convert2OpenHuFuType(call.getType().getSqlTypeName());
      return ExpressionFactory.createUnaryOperator(op, type, in);
    }

    /**
     * translate case
     */
    Expression caseCall(RexCall call) {
      // in Case Rexcall, oprands are organized as [when, then, when, then, ..., else]
      OperatorType op = OperatorType.CASE;
      List<Expression> eles =
          call.operands.stream().map(c -> convert(c)).collect(Collectors.toList());
      ColumnType type = TypeConverter.convert2OpenHuFuType(call.getType().getSqlTypeName());
      return ExpressionFactory.createMultiOperator(op, type, eles);
    }

    Expression searchCall(RexCall call) {
      Expression in = convert(call.operands.get(0));
      RexLiteral ranges = (RexLiteral) call.operands.get(1);
      return convertRangeSet((Sarg) ranges.getValue2(),
          TypeConverter.convert2OpenHuFuType(ranges.getType().getSqlTypeName()), in);
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
            Object lowerEndpoint = r.lowerEndpoint() instanceof DateString ?
                ((DateString) r.lowerEndpoint()).toCalendar():r.lowerEndpoint();
            Object upperEndpoint = r.upperEndpoint() instanceof DateString ?
                ((DateString) r.upperEndpoint()).toCalendar():r.upperEndpoint();

            rangeExps.add(convertRange(lowerEndpoint, upperEndpoint, r.lowerBoundType(),
                r.upperBoundType(), ColumnType.DATE, in));
            break;
          case TIME:
            rangeExps.add(convertRange(r.lowerEndpoint(), r.upperEndpoint(), r.lowerBoundType(),
                r.upperBoundType(), ColumnType.TIME, in));
            break;
          case TIMESTAMP:
            rangeExps.add(convertRange(r.lowerEndpoint(), r.upperEndpoint(), r.lowerBoundType(),
                r.upperBoundType(), ColumnType.TIMESTAMP, in));
            break;
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
      return ExpressionUtils.disjunctCondtion(rangeExps);
    }

    public static Expression convertRange(Object left, Object right, BoundType leftBound,
        BoundType rightBound, ColumnType cType, Expression in) {
      if (cType.equals(ColumnType.STRING)) {
        if (left instanceof NlsString) {
          left = ((NlsString) left).getValue();
        }
        if (right instanceof NlsString) {
          right = ((NlsString) right).getValue();
        }
      }
      if (left.equals(right) && leftBound.equals(BoundType.CLOSED)
          && rightBound.equals(BoundType.CLOSED)) {
        return ExpressionFactory.createBinaryOperator(OperatorType.EQ, ColumnType.BOOLEAN, in,
            ExpressionFactory.createLiteral(cType, left));
      }
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

    /**
     * translate localref
     */
    Expression localRef(RexLocalRef node) {
      RexNode local = locals.get(node.getIndex());
      // todo: this can be optimized
      return convert(local);
    }

    /**
     * translate func
     */
    Expression scalarFunc(RexCall call) {
      SqlUserDefinedFunction function = (SqlUserDefinedFunction) call.op;
      String funcName = function.getName().toLowerCase();
      List<Expression> eles =
          call.operands.stream().map(r -> convert(r)).collect(Collectors.toList());
      switch (funcName) {
        case "abs":
          // todo: add more scalar function here
          ColumnType type = TypeConverter.convert2OpenHuFuType(call.getType().getSqlTypeName());
          return ExpressionFactory.createScalarFunc(type, function.getName(), eles);
        default:
          if (!UDFLoader.scalarUDFs.containsKey(funcName)) {
            throw new RuntimeException("can't translate " + call);
          } else {
            return ExpressionFactory.createScalarFunc(UDFLoader.getScalarOutType(funcName, eles), function.getName(), eles);
          }
      }
    }
  }
}
