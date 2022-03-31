package com.hufudb.onedb.core.sql.expression;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.TypeConverter;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;

/*
* tree node of expression tree
*/
public class OneDBOperator implements OneDBExpression {
  public enum FuncType {
    NONE,
    ABS;
    public static FuncType of(int id) {
      return FuncType.values()[id];
    }
  }

  OneDBOpType opType;
  FieldType outType;
  List<OneDBExpression>inputs;
  FuncType funcType;

  OneDBOperator(OneDBOpType opType, FieldType outType, List<OneDBExpression> inputs, FuncType funcType) {
    this.opType = opType;
    this.outType = outType;
    this.inputs = inputs;
    this.funcType = funcType;
  }

  public ExpressionProto toProto() {
    ExpressionProto.Builder builder = ExpressionProto.newBuilder();
    builder.setOpType(opType.ordinal());
    builder.setOutType(outType.ordinal());
    builder.addAllIn(inputs.stream().map(e -> e.toProto()).collect(Collectors.toList()));
    builder.setFunc(funcType.ordinal());
    return builder.build();
  }

  public List<OneDBExpression> getInputs() {
    return inputs;
  }

  public FuncType getFuncType() {
    return funcType;
  }

  /*
  * functions to build operator tree
  */

  public static OneDBExpression fromRexNode(RexNode node) {
    return new OperatorBuilder(ImmutableList.of(node)).build().get(0);
  }

  public static OneDBExpression fromRexNode(RexNode node, List<OneDBExpression> ins) {
    return new OperatorBuilder(ImmutableList.of(node), ins).build().get(0);
  }

  public static List<OneDBExpression> fromRexNodes(List<RexNode> nodes, List<OneDBExpression> ins) {
    return nodes.stream().map(node -> {
      if (node instanceof RexInputRef) {
        int i = ((RexInputRef)node).getIndex();
        return new OneDBReference(ins.get(i).getOutType(), i);
      } else {
        return OneDBOperator.fromRexNode(node, ins);
      }
    }).collect(Collectors.toList());
  }

  public static List<OneDBExpression> fromRexNodes(RexProgram program, List<OneDBExpression> ins) {
    return new OperatorBuilder(program.getExprList(), program.getProjectList(), ins).build();
  }

  static private class OperatorBuilder {
    List<? extends RexNode> outputNodes;
    List<RexNode> localNodes;
    List<OneDBExpression> inputExps;

    OperatorBuilder(List<RexNode> nodes) {
      this.outputNodes = nodes;
      this.localNodes = nodes;
      this.inputExps = ImmutableList.of();
    }

    OperatorBuilder(List<RexNode> nodes, List<OneDBExpression> inputs) {
      this.outputNodes = nodes;
      this.localNodes = nodes;
      this.inputExps = inputs;
    }

    OperatorBuilder(List<RexNode> localNodes, List<? extends RexNode> outputNodes, List<OneDBExpression> inputs) {
      this.outputNodes = outputNodes;
      this.localNodes = localNodes;
      this.inputExps = inputs;
    }

    List<OneDBExpression> build() {
      return outputNodes.stream().map(node -> buildOp(node)).collect(Collectors.toList());
    }

    OneDBExpression buildOp(RexNode node) {
      switch(node.getKind()) {
        // leaf node
        case LITERAL:
        case INPUT_REF:
          return leaf(node);
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
          return unary((RexCall) node);
        // local_ref
        case LOCAL_REF:
          return localRef((RexLocalRef) node);
        case OTHER_FUNCTION:
          return scalarFunc((RexCall) node);
        default:
          throw new RuntimeException(String.format("not support %s", node));
      }
    }

    /*
    * only accept literal and input reference node
    */
    OneDBExpression leaf(RexNode node) {
      switch (node.getKind()) {
        case LITERAL:
          return OneDBLiteral.fromLiteral((RexLiteral)node);
        case INPUT_REF:
          if (inputExps.isEmpty()) {
            return OneDBReference.fromInputRef((RexInputRef)node);
          } else {
            return inputExps.get(((RexInputRef)node).getIndex());
          }
        default:
          throw new RuntimeException("can't translate " + node);
      }
    }

    /*
    * add binary operator
    */
    OneDBExpression binary(RexCall call) {
      OneDBOpType op;
      switch (call.getKind()) {
        case GREATER_THAN:
          op = OneDBOpType.GT;
          break;
        case GREATER_THAN_OR_EQUAL:
          op = OneDBOpType.GE;
          break;
        case LESS_THAN:
          op = OneDBOpType.LT;
          break;
        case LESS_THAN_OR_EQUAL:
          op = OneDBOpType.LE;
          break;
        case EQUALS:
          op = OneDBOpType.EQ;
          break;
        case NOT_EQUALS:
          op = OneDBOpType.NE;
          break;
        case PLUS:
          op = OneDBOpType.PLUS;
          break;
        case MINUS:
          op = OneDBOpType.MINUS;
          break;
        case TIMES:
          op = OneDBOpType.TIMES;
          break;
        case DIVIDE:
          op = OneDBOpType.DIVIDE;
          break;
        case MOD:
          op = OneDBOpType.MOD;
          break;
        case AND:
          op = OneDBOpType.AND;
          break;
        case OR:
          op = OneDBOpType.OR;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      List<OneDBExpression> eles = ImmutableList.of(buildOp(call.operands.get(0)), buildOp(call.operands.get(1)));
      FieldType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBOperator(op, type, eles, FuncType.NONE);
    }

    /*
    * add unary operator
    */
    OneDBExpression unary(RexCall call) {
      OneDBOpType op;
      switch (call.getKind()) {
        case AS:
          op = OneDBOpType.AS;
        case NOT:
          op = OneDBOpType.NOT;
          break;
        case PLUS_PREFIX:
          op = OneDBOpType.PLUS_PRE;
          break;
        case MINUS_PREFIX:
          op = OneDBOpType.MINUS_PRE;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      List<OneDBExpression> eles = ImmutableList.of(buildOp(call.operands.get(0)));
      FieldType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBOperator(op, type, eles, FuncType.NONE);
    }

    /*
    * translate localref
    */
    OneDBExpression localRef(RexLocalRef node) {
      RexNode local = localNodes.get(node.getIndex());
      // todo: this can be optimized
      return buildOp(local);
    }

    /*
    * translate func
    */
    OneDBExpression scalarFunc(RexCall call) {
      OneDBOpType op = OneDBOpType.SCALAR_FUNC;
      SqlUserDefinedFunction function = (SqlUserDefinedFunction) call.op;
      FuncType func;
      switch (function.getName()) {
        case "ABS":
          func = FuncType.ABS;
          break;
        default:
          throw new RuntimeException("can't translate " + call);
      }
      List<OneDBExpression> eles = call.operands.stream().map(r -> buildOp(r)).collect(Collectors.toList());
      FieldType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBOperator(op, type, eles, func);
    }
  }

  @Override
  public FieldType getOutType() {
    return outType;
  }

  @Override
  public OneDBOpType getOpType() {
    return opType;
  }
}