package com.hufudb.onedb.core.sql.expression;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Row;

// plaintext interpreter for onedb expression
public class ExpressionInterpreter {

  public static Comparable implement(Row row, OneDBExpression e) {
    switch (e.getOpType()) {
      case REF:
        return implementRef(row, (OneDBReference) e);
      case LITERAL:
        return implementLiteral(row, (OneDBLiteral) e);
      case AGG_FUNC:
        throw new UnsupportedOperationException("aggregate not support in interpereter");
      default:
        return implementOperator(row, (OneDBOperator) e);
    }
  }

  static Comparable implementRef(Row row, OneDBReference ref) {
    return (Comparable) row.getObject(ref.getIdx());
  }

  static Comparable implementLiteral(Row row, OneDBLiteral literal) {
    return (Comparable) literal.getValue();
  }

  static Comparable implementOperator(Row row, OneDBOperator op) {
    List<Comparable> inputs = op.getInputs().stream().map(exp -> implement(row, exp)).collect(Collectors.toList());
    switch (op.getOpType()) {
      // binary
      case GT:
        return (Boolean)(inputs.get(0).compareTo(inputs.get(1)) > 0);
      case GE:
        return (Boolean)(inputs.get(0).compareTo(inputs.get(1)) >= 0);
      case LT:
        return (Boolean)(inputs.get(0).compareTo(inputs.get(1)) < 0);
      case LE:
        return (Boolean)(inputs.get(0).compareTo(inputs.get(1)) <= 0);
      case EQ:
        return (Boolean)(inputs.get(0).compareTo(inputs.get(1)) == 0);
      case NE:
        return (Boolean)(inputs.get(0).compareTo(inputs.get(1)) != 0);
      case PLUS:
        return number(inputs.get(0)).add(number(inputs.get(1)));
      case MINUS:
        return number(inputs.get(0)).subtract(number(inputs.get(1)));
      case TIMES:
        return number(inputs.get(0)).multiply(number(inputs.get(1)));
      case DIVIDE:
        return number(inputs.get(0)).divide(number(inputs.get(1)));
      case MOD:
        return number(inputs.get(0)).remainder(number(inputs.get(1)));
      case AND:
        return ((Boolean) inputs.get(0)) && ((Boolean) inputs.get(1));
      case OR:
        return ((Boolean) inputs.get(0)) || ((Boolean) inputs.get(1));
      // unary
      case AS:
        return inputs.get(0);
      case PLUS_PRE:
        return number(inputs.get(0)).plus();
      case MINUS_PRE:
        return number(inputs.get(0)).negate();
      case NOT:
        return (Boolean) inputs.get(0);
      // todo: support scalar functions
      default:
        throw new UnsupportedOperationException("operator not support in intereperter");
    }
  }

  private static BigDecimal number(Comparable comparable) {
    return comparable instanceof BigDecimal
        ? (BigDecimal) comparable
        : comparable instanceof BigInteger
            ? new BigDecimal((BigInteger) comparable)
            : comparable instanceof Long
                || comparable instanceof Integer
                || comparable instanceof Short
                    ? new BigDecimal(((Number) comparable).longValue())
                    : new BigDecimal(((Number) comparable).doubleValue());
  }

  public static Comparable cast(Comparable in, final FieldType type) {
    switch(type) {
      case STRING:
        return in;
      case BOOLEAN:
        return in;
      case BYTE:
        return number(in).byteValue();
      case SHORT:
        return number(in).shortValue();
      case INT:
        return number(in).intValue();
      case DATE:
      case TIME:
      case TIMESTAMP:
      case LONG:
        return number(in).longValue();
      case FLOAT:
        return number(in).floatValue();
      case DOUBLE:
        return number(in).doubleValue();
      case POINT:
        return in;
      default:
        throw new UnsupportedOperationException("field type not support");
    }
  }
}
