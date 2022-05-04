package com.hufudb.onedb.core.sql.translator;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.SearchList;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBLiteral;
import com.hufudb.onedb.core.sql.expression.OneDBOpType;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import com.hufudb.onedb.core.sql.expression.OneDBOperator.FuncType;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import com.hufudb.onedb.core.sql.expression.OneDBReference;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall.AggregateType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OneDBTranslator {
  Header inputHeader;
  List<String> inputExps;
  List<OneDBExpression> exps;

  OneDBTranslator(Header inputHeader, List<OneDBExpression> exps) {
    this.inputHeader = inputHeader;
    this.exps = exps;
  }

  OneDBTranslator(List<String> exps, List<OneDBExpression> aggs) {
    this.inputExps = exps;
    this.exps = aggs;
  }

  public static List<String> translateOrders(List<String> exps, List<OneDBOrder> orders) {
    return orders.stream()
        .map(order -> String.format("%s %s", exps.get(order.inputRef), order.direction.toString()))
        .collect(Collectors.toList());
  }

  public static List<String> translateExps(Header inputHeader, List<OneDBExpression> exps) {
    return new OneDBTranslator(inputHeader, exps).translateAllExps();
  }

  public static List<String> translateAgg(List<String> exps, List<OneDBExpression> aggs) {
    return new OneDBTranslator(exps, aggs).translateAgg();
  }

  List<String> translateAllExps() {
    return exps.stream().map(exp -> translate(exp)).collect(Collectors.toList());
  }

  public List<String> translateAgg() {
    return exps.stream().map(exp -> aggregateFunc((OneDBAggCall) exp)).collect(Collectors.toList());
  }

  protected String translate(OneDBExpression exp) {
    switch (exp.getOpType()) {
      case REF:
        return inputRef((OneDBReference) exp);
      case LITERAL:
        return literal((OneDBLiteral) exp);
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
        return binary((OneDBOperator) exp);
      // unary
      case AS:
      case NOT:
      case PLUS_PRE:
      case MINUS_PRE:
      case IS_NULL:
      case IS_NOT_NULL:
        return unary((OneDBOperator) exp);
      case CASE:
        return caseCall((OneDBOperator) exp);
      case SEARCH:
        return searchCall((OneDBOperator) exp);
      case SCALAR_FUNC:
        return scalarFunc((OneDBOperator) exp);
      default:
        throw new RuntimeException("can't translate " + exp);
    }
  }

  protected String inputRef(OneDBReference ref) {
    int idx = ref.getIdx();
    return inputHeader.getName(idx);
  }

  protected String literal(OneDBLiteral literal) {
    FieldType type = literal.getOutType();
    switch (type) {
      case BOOLEAN:
        return String.valueOf(literal.getValue());
      case BYTE:
      case SHORT:
      case INT:
        return String.valueOf(literal.getValue());
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        return String.valueOf(literal.getValue());
      case FLOAT:
        return String.valueOf(literal.getValue());
      case DOUBLE:
        return String.valueOf(literal.getValue());
      case STRING:
        return String.format("'%s'", literal.getValue());
      default:
        throw new RuntimeException("can't translate literal " + literal);
    }
  }

  protected String unary(OneDBOperator exp) {
    OneDBOpType type = exp.getOpType();
    String in = translate(exp.getInputs().get(0));
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
        return String.format("(%s IS NOT NULL", in);
      case NOT:
        return String.format("(NOT %s)", in);
      default:
        throw new RuntimeException("can't translate unary " + exp);
    }
  }

  protected String binary(OneDBOperator exp) {
    String left = translate(exp.getInputs().get(0));
    String right = translate(exp.getInputs().get(1));
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
      default:
        throw new RuntimeException("can't translate binary " + exp);
    }
    return String.format("(%s %s %s)", left, op, right);
  }

  protected String caseCall(OneDBOperator exp) {
    List<String> inputs =
        exp.getInputs().stream().map(e -> translate(e)).collect(Collectors.toList());
    List<String> caseList = new ArrayList<>();
    for (int i = 1; i < inputs.size(); i += 2) {
      caseList.add(String.format("WHEN %s THEN %s", inputs.get(i - 1), inputs.get(i)));
    }
    String elseCase = String.format("ELSE %s", inputs.get(inputs.size() - 1));
    return String.format("CASE %s %s END", String.join(" ", caseList), elseCase);
  }

  protected String searchCall(OneDBOperator exp) {
    List<OneDBExpression> inputs = exp.getInputs();
    assert inputs.size() == 2 && inputs.get(1).getOpType() == OneDBOpType.LITERAL;
    String left = translate(inputs.get(0));
    List<String> searchClause =
        ((SearchList) ((OneDBLiteral) inputs.get(1)).getValue()).toSqlString();
    for (int i = 0; i < searchClause.size(); i++) {
      searchClause.set(i, left + searchClause.get(i));
    }
    return String.join(" or ", searchClause);
  }


  protected String scalarFunc(OneDBOperator exp) {
    FuncType func = exp.getFuncType();
    List<String> inputs =
        exp.getInputs().stream().map(e -> translate(e)).collect(Collectors.toList());
    switch (func) {
      case ABS:
        if (inputs.size() != 1) {
          throw new RuntimeException("ABS need 1 arguements, but give " + inputs.size());
        }
        return String.format("ABS(%s)", inputs.get(0));
      default:
        throw new RuntimeException("can't translate scalarFunc " + exp);
    }
  }

  protected String aggregateFunc(OneDBAggCall exp) {
    AggregateType type = exp.getAggType();
    List<Integer> inputRefs = exp.getInputRef();
    switch (type) {
      case GROUPKEY:
        return inputExps.get(inputRefs.get(0));
      case SUM:
        return String.format("SUM(%s)", inputExps.get(inputRefs.get(0)));
      case COUNT:
        if (inputRefs.isEmpty()) {
          return String.format("COUNT(*)");
        } else if (inputRefs.size() == 1) {
          return String.format("COUNT(%s)", inputExps.get(exp.getInputRef().get(0)));
        } else {
          List<String> inputs =
              inputRefs.stream().map(ref -> inputExps.get(ref)).collect(Collectors.toList());
          return String.format("COUNT((%s))", String.join(",", inputs));
        }
      case AVG:
        return String.format("AVG(%s)", inputExps.get(exp.getInputRef().get(0)));
      case MAX:
        return String.format("MAX(%s)", inputExps.get(exp.getInputRef().get(0)));
      case MIN:
        return String.format("MIN(%s)", inputExps.get(exp.getInputRef().get(0)));
      default:
        throw new RuntimeException("can't translate aggFunc " + exp);
    }
  }
}
