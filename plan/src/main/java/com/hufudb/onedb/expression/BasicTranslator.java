package com.hufudb.onedb.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.storage.utils.DateUtils;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import com.hufudb.onedb.udf.UDFLoader;

public class BasicTranslator implements Translator {
  protected String dataSource;
  protected List<String> inputStrs;

  public BasicTranslator(String dataSource) {
    this.dataSource = dataSource;
    this.inputStrs = ImmutableList.of();
  }

  public void setInput(List<String> inputs) {
    this.inputStrs = inputs;
  }

  public String translate(Expression exp) {
    switch (exp.getOpType()) {
      case REF:
        return inputRef(exp);
      case LITERAL:
        return literal(exp);
      case PLUS:
      case MINUS:
      // binary
      case GT:
      case GE:
      case LT:
      case LE:
      case EQ:
      case NE:
      case TIMES:
      case DIVIDE:
      case MOD:
      case AND:
      case OR:
      case LIKE:
        return binary(exp);
      // unary
      case AS:
      case NOT:
      case PLUS_PRE:
      case MINUS_PRE:
      case IS_NULL:
      case IS_NOT_NULL:
        return unary(exp);
      case CASE:
        return caseCall(exp);
      case SCALAR_FUNC:
        return scalarFunc(exp);
      case AGG_FUNC:
        return aggregateFunc(exp);
      default:
        throw new RuntimeException("can't translate " + exp);
    }
  }

  protected String inputRef(Expression ref) {
    int idx = ref.getI32();
    return inputStrs.get(idx);
  }

  protected String literal(Expression literal) {
    ColumnType type = literal.getOutType();
    switch (type) {
      case BOOLEAN:
        return String.valueOf(literal.getB());
      case BYTE:
      case SHORT:
      case INT:
        return String.valueOf(literal.getI32());
      case DATE:
        return String.format("date '%s'", DateUtils.intToDate(literal.getI32()).toString());
      case TIME:
        return String.format("time '%s'", DateUtils.intToTime(literal.getI32()).toString());
      case TIMESTAMP:
        return String.format("timestamp '%s'", DateUtils.longToTimestamp(literal.getI64()).toString());
      case LONG:
        return String.valueOf(literal.getI64());
      case FLOAT:
        return String.valueOf(literal.getF32());
      case DOUBLE:
        return String.valueOf(literal.getF64());
      case STRING:
        return String.format("'%s'", literal.getStr());
      default:
        throw new RuntimeException("can't translate literal " + literal);
    }
  }

  protected String unary(Expression exp) {
    OperatorType type = exp.getOpType();
    String in = translate(exp.getIn(0));
    switch (type) {
      case AS:
        return String.format("(%s)", in);
      case PLUS_PRE:
        return String.format("(+%s)", in);
      case MINUS_PRE:
        return String.format("(-%s)", in);
      case IS_NULL:
        return String.format("(%s IS NULL)", in);
      case IS_NOT_NULL:
        return String.format("(%s IS NOT NULL)", in);
      case NOT:
        return String.format("(NOT %s)", in);
      default:
        throw new RuntimeException("can't translate unary " + exp);
    }
  }

  protected String binary(Expression exp) {
    String left = translate(exp.getIn(0));
    String right = translate(exp.getIn(1));
    String op;
    switch (exp.getOpType()) {
      case GT:
        op = ">";
        break;
      case GE:
        op = ">=";
        break;
      case LT:
        op = "<";
        break;
      case LE:
        op = "<=";
        break;
      case EQ:
        op = "=";
        break;
      case NE:
        op = "<>";
        break;
      case PLUS:
        op = "+";
        break;
      case MINUS:
        op = "-";
        break;
      case TIMES:
        op = "*";
        break;
      case DIVIDE:
        op = "/";
        break;
      case MOD:
        op = "%";
        break;
      case AND:
        op = "AND";
        break;
      case OR:
        op = "OR";
        break;
      case LIKE:
        op = "LIKE";
        break;
      default:
        throw new RuntimeException("can't translate binary " + exp);
    }
    return String.format("(%s %s %s)", left, op, right);
  }

  protected String caseCall(Expression exp) {
    List<String> inputs =
        exp.getInList().stream().map(e -> translate(e)).collect(Collectors.toList());
    List<String> caseList = new ArrayList<>();
    for (int i = 1; i < inputs.size(); i += 2) {
      caseList.add(String.format("WHEN %s THEN %s", inputs.get(i - 1), inputs.get(i)));
    }
    String elseCase = String.format("ELSE %s", inputs.get(inputs.size() - 1));
    return String.format("CASE %s %s END", String.join(" ", caseList), elseCase);
  }

  protected String scalarFunc(Expression exp) {
    List<String> inputs =
        exp.getInList().stream().map(e -> translate(e)).collect(Collectors.toList());
    if (ScalarFuncType.support(exp.getStr())) {
      ScalarFuncType func = ScalarFuncType.of(exp.getStr());
      switch (func) {
        case ABS:
          if (inputs.size() != 1) {
            throw new RuntimeException("ABS need 1 arguements, but give " + inputs.size());
          }
          return String.format("ABS(%s)", inputs.get(0));
        default:
          throw new RuntimeException("Unsupported scalar function");
      }
    }
    return UDFLoader.translateScalar(exp.getStr(), dataSource, inputs);
  }

  protected String aggregateFunc(Expression exp) {
    AggFuncType type = AggFuncType.of(exp.getI32());
    List<Integer> inputRefs = ExpressionUtils.getAggInputs(exp);
    switch (type) {
      case GROUPKEY:
        return inputStrs.get(inputRefs.get(0));
      case SUM:
        return String.format("SUM(%s)", inputStrs.get(inputRefs.get(0)));
      case COUNT:
        if (inputRefs.isEmpty()) {
          return String.format("COUNT(*)");
        } else if (inputRefs.size() == 1) {
          return String.format("COUNT(%s)", inputStrs.get(inputRefs.get(0)));
        } else {
          List<String> inputs =
              inputRefs.stream().map(ref -> inputStrs.get(ref)).collect(Collectors.toList());
          return String.format("COUNT((%s))", String.join(",", inputs));
        }
      case AVG:
        return String.format("AVG(%s)", inputStrs.get(inputRefs.get(0)));
      case MAX:
        return String.format("MAX(%s)", inputStrs.get(inputRefs.get(0)));
      case MIN:
        return String.format("MIN(%s)", inputStrs.get(inputRefs.get(0)));
      default:
        throw new RuntimeException("can't translate aggFunc " + exp);
    }
  }
}
