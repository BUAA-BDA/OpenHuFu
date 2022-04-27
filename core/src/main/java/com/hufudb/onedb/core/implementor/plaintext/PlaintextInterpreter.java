package com.hufudb.onedb.core.implementor.plaintext;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBLiteral;
import com.hufudb.onedb.core.sql.expression.OneDBOpType;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import com.hufudb.onedb.core.sql.expression.OneDBReference;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;
import java.util.stream.Collectors;

// plaintext interpreter for onedb expression
public class PlaintextInterpreter {

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
    List<OneDBExpression> eles = op.getInputs();
    switch (op.getOpType()) {
      // boolean
      case AND:
      case OR:
      case NOT:
        return calBoolean(row, op.getOpType(), eles);
      // compare
      case GT:
      case GE:
      case LT:
      case LE:
      case EQ:
      case NE:
        return compare(row, op.getOpType(), eles.get(0), eles.get(1));
      // calculate
      case PLUS:
      case MINUS:
      case TIMES:
      case DIVIDE:
      case MOD:
      case PLUS_PRE:
      case MINUS_PRE:
        return calculate(row, op.getOpType(), eles);
      // unary
      case AS:
        return implement(row, eles.get(0));
      case IS_NULL:
        return implement(row, eles.get(0)) == null;
      case IS_NOT_NULL:
        return implement(row, eles.get(0)) != null;
      case CASE:
        for (int i = 1; i < eles.size(); i += 2) {
          if ((Boolean) implement(row, eles.get(i - 1))) {
            return implement(row, eles.get(i));
          }
        }
        return implement(row, eles.get(eles.size() - 1));
      // todo: support scalar functions
      default:
        throw new UnsupportedOperationException("operator not support in intereperter");
    }
  }

  private static BigDecimal number(Comparable comparable) {
    return comparable instanceof BigDecimal ? (BigDecimal) comparable
        : comparable instanceof BigInteger ? new BigDecimal((BigInteger) comparable)
            : comparable instanceof Long || comparable instanceof Integer
                || comparable instanceof Short ? new BigDecimal(((Number) comparable).longValue())
                    : new BigDecimal(((Number) comparable).doubleValue());
  }

  public static Comparable compare(Row row, OneDBOpType compType, OneDBExpression leftExp,
      OneDBExpression rightExp) {
    Comparable left = implement(row, leftExp);
    Comparable right = implement(row, rightExp);
    if (left == null || right == null) {
      return null;
    }

    if (left instanceof Number) {
      left = number(left);
    }
    if (right instanceof Number) {
      right = number(right);
    }
    // noinspection unchecked
    final int c = left.compareTo(right);
    switch (compType) {
      case GT:
        return c > 0;
      case GE:
        return c >= 0;
      case LT:
        return c < 0;
      case LE:
        return c <= 0;
      case EQ:
        return c == 0;
      case NE:
        return c != 0;
      default:
        throw new UnsupportedOperationException("not support compare type");
    }
  }

  public static Comparable calBoolean(Row row, OneDBOpType opType, List<OneDBExpression> inputs) {
    switch (opType) {
      case AND: {
        Comparable left = implement(row, inputs.get(0));
        if (left == null) {
          return null;
        } else if (left.equals(false)) {
          return false;
        }
        Comparable right = implement(row, inputs.get(1));
        if (right == null) {
          return null;
        } else {
          return right.equals(true);
        }
      }
      case OR: {
        Comparable left = implement(row, inputs.get(0));
        if (left.equals(true)) {
          return true;
        }
        Comparable right = implement(row, inputs.get(1));
        if (right.equals(true)) {
          return true;
        } else if (left == null || right == null) {
          return null;
        } else {
          return false;
        }
      }
      case NOT: {
        Comparable v = implement(row, inputs.get(0));
        if (v == null) {
          return v;
        } else {
          return !(Boolean) v;
        }
      }
      default:
        throw new UnsupportedOperationException("not support bool operator");
    }
  }

  public static Comparable calculate(Row row, OneDBOpType opType, List<OneDBExpression> exps) {
    List<Comparable> inputs =
        exps.stream().map(e -> implement(row, e)).collect(Collectors.toList());
    for (Comparable in : inputs) {
      if (in == null) {
        return null;
      }
    }
    switch (opType) {
      case PLUS:
        return number(inputs.get(0)).add(number(inputs.get(1)));
      case MINUS:
        return number(inputs.get(0)).subtract(number(inputs.get(1)));
      case TIMES:
        return number(inputs.get(0)).multiply(number(inputs.get(1)));
      case DIVIDE:
        return number(inputs.get(0)).divide(number(number(inputs.get(1))), MathContext.DECIMAL64);
      case MOD:
        return number(inputs.get(0)).remainder(number(inputs.get(1)));
      case PLUS_PRE:
        return number(inputs.get(0)).plus();
      case MINUS_PRE:
        return number(inputs.get(0)).negate();
      default:
        throw new UnsupportedOperationException("not support op type");
    }
  }

  public static Comparable cast(Comparable in, final FieldType type) {
    if (in == null) {
      return in;
    }
    switch (type) {
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
