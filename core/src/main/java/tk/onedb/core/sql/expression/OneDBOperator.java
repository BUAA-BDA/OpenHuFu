package tk.onedb.core.sql.expression;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import tk.onedb.core.data.FieldType;
import tk.onedb.core.data.TypeConverter;
import tk.onedb.rpc.OneDBCommon.ExpressionProto;

/*
* tree node of expression tree
*/
public class OneDBOperator implements OneDBExpression {
  public enum OperatorType {
    REF, // for leaf node
    AS,
    LITERAL,
    PLUS,
    MINUS,
    TIMES,
    DIVIDE,
    MOD,
    GT,
    GE,
    LT,
    LE,
    EQ,
    NE,
    AND,
    OR,
    NOT,
    XOR,
    SCALAR_FUNC;

    public static OperatorType of(int id) {
      return OperatorType.values()[id];
    }
  }

  public enum FuncType {
    NONE,
    ABS;
    public static FuncType of(int id) {
      return FuncType.values()[id];
    }
  }

  OperatorType opType;
  FieldType outType;
  List<OneDBExpression>inputs;
  FuncType funcType;

  OneDBOperator(OperatorType opType, FieldType outType, List<OneDBExpression> inputs, FuncType funcType) {
    this.opType = opType;
    this.outType = outType;
    this.inputs = inputs;
    this.funcType = funcType;
  }

  public static OneDBExpression fromProto(ExpressionProto proto) {
    OperatorType opType = OperatorType.of(proto.getOpType());
    FuncType funcType = FuncType.of(proto.getFunc());
    FieldType outType = FieldType.of(proto.getOutType());
    List<OneDBExpression> elements = proto.getInList().stream().map(ele -> OneDBElement.fromProto(ele)).collect(Collectors.toList());
    return new OneDBOperator(opType, outType, elements,funcType);
  }

  public ExpressionProto toProto() {
    ExpressionProto.Builder builder = ExpressionProto.newBuilder();
    builder.setOpType(opType.ordinal());
    builder.setOutType(outType.ordinal());
    builder.addAllIn(inputs.stream().map(e -> e.toProto()).collect(Collectors.toList()));
    builder.setFunc(funcType.ordinal());
    return builder.build();
  }

  /*
  * functions to build operator tree
  */

  public static OneDBExpression fromRexNode(RexNode node) {
    return new OperatorBuilder(ImmutableList.of(node)).build().get(0);
  }

  static private class OperatorBuilder {
    List<RexNode> nodes;

    OperatorBuilder(List<RexNode> nodes) {
      this.nodes = nodes;
    }

    List<OneDBExpression> build() {
      return nodes.stream().map(node -> buildOp(node)).collect(Collectors.toList());
    }

    OneDBExpression buildOp(RexNode node) {
      switch(node.getKind()) {
        // leaf node
        case LITERAL:
        case INPUT_REF:
          return as(node);
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
    OneDBExpression as(RexNode node) {
      switch (node.getKind()) {
        case LITERAL:
          return OneDBElement.fromLiteral((RexLiteral)node);
        case INPUT_REF:
          return OneDBElement.fromInputRef((RexInputRef)node);
        default:
          throw new RuntimeException("can't translate " + node);
      }
    }

    /*
    * add binary operator
    */
    OneDBExpression binary(RexCall call) {
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
      List<OneDBExpression> eles = ImmutableList.of(buildOp(call.operands.get(0)), buildOp(call.operands.get(1)));
      FieldType type = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      return new OneDBOperator(op, type, eles, FuncType.NONE);
    }

    /*
    * add unary operator
    */
    OneDBExpression unary(RexCall call) {
      OperatorType op;
      switch (call.getKind()) {
        case AS:
          op = OperatorType.AS;
        case NOT:
          op = OperatorType.NOT;
          break;
        case PLUS_PREFIX:
          op = OperatorType.PLUS;
          break;
        case MINUS_PREFIX:
          op = OperatorType.MINUS;
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
      RexNode local = nodes.get(node.getIndex());
      // todo: this can be optimized
      return buildOp(local);
    }

    /*
    * translate func
    */
    OneDBExpression scalarFunc(RexCall call) {
      OperatorType op = OperatorType.SCALAR_FUNC;
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
}