package com.hufudb.onedb.interpreter;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.function.Filter;
import com.hufudb.onedb.data.function.Mapper;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.FilterDataSet;
import com.hufudb.onedb.data.storage.MapDataSet;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.expression.ExpressionUtils;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;

public class Interpreter {
  private Interpreter() {
  }

  public static DataSet filter(DataSet source, List<Expression> conditions) {
    if (conditions.isEmpty()) {
      return source;
    } else {
      final Expression condition = ExpressionUtils.conjunctCondition(conditions);
      final Schema schema = source.getSchema();
      return new FilterDataSet(schema, new InterpretiveFilter(schema, condition), source);
    }
  }

  public static DataSet map(DataSet source, List<Expression> exps) {
    if (exps.isEmpty()) {
      return source;
    } else {
      final Schema sourceSchema = source.getSchema();
      final Schema outSchema = ExpressionUtils.createSchema(exps);
      List<Mapper> maps = exps.stream().map(exp -> new InterpretivMapper(sourceSchema, exp))
          .collect(Collectors.toList());
      return MapDataSet.create(outSchema, maps, source);
    }
  }

  public static class InterpretiveFilter implements Filter {
    final Schema schema;
    final Expression condition;

    public InterpretiveFilter(Schema schema, Expression condition) {
      this.schema = schema;
      this.condition = condition;
    }

    @Override
    public boolean filter(Row row) {
      return (boolean) implement(row, condition);
    }
  }

  public static class InterpretivMapper implements Mapper {
    final Schema schema;
    final Expression exp;

    public InterpretivMapper(Schema schema, Expression exp) {
      this.schema = schema;
      this.exp = exp;
    }

    @Override
    public Object map(Row row) {
      Object r = implement(row, exp);
      if (r instanceof Number) {
        return cast(exp.getOutType(), (Number) r);
      }
      return r;
    }
  }

  public static Object implement(Row row, Expression e) {
    final List<Expression> inputs = e.getInList();
    final OperatorType type = e.getOpType();
    switch (type) {
      case REF:
        return row.get(e.getI32());
      case LITERAL:
        return ExpressionUtils.getLiteral(e);
      case AND:
      case OR:
      case NOT:
        return calBoolean(row, type, inputs);
      case GT:
      case GE:
      case LT:
      case LE:
      case EQ:
      case NE:
        return calCompare(row, type, inputs);
      case PLUS:
      case MINUS:
      case TIMES:
      case DIVIDE:
      case MOD:
      case PLUS_PRE:
      case MINUS_PRE:
        return calculate(row, type, inputs);
      case AS:
        return implement(row, inputs.get(0));
      case IS_NULL:
        return implement(row, inputs.get(0)) == null;
      case IS_NOT_NULL:
        return implement(row, inputs.get(0)) != null;
      case CASE:
        for (int i = 1; i < inputs.size(); i += 2) {
          if ((Boolean) implement(row, inputs.get(i - 1))) {
            return implement(row, inputs.get(i));
          }
        }
        return implement(row, inputs.get(inputs.size() - 1));
      default:
        throw new UnsupportedOperationException("operator not support in intereperter");
    }
  }

  private static Boolean calBoolean(Row row, OperatorType type, List<Expression> inputs) {
    switch (type) {
      case AND: {
        Object left = implement(row, inputs.get(0));
        if (left == null) {
          return null;
        } else if (left.equals(false)) {
          return false;
        }
        Object right = implement(row, inputs.get(1));
        if (right == null) {
          return null;
        } else {
          return right.equals(true);
        }
      }
      case OR: {
        Object left = implement(row, inputs.get(0));
        if (left.equals(true)) {
          return true;
        }
        Object right = implement(row, inputs.get(1));
        if (right.equals(true)) {
          return true;
        } else if (left == null || right == null) {
          return null;
        } else {
          return false;
        }
      }
      case NOT: {
        Object v = implement(row, inputs.get(0));
        if (v == null) {
          return null;
        } else {
          return !(boolean) v;
        }
      }
      default:
        throw new UnsupportedOperationException("not support bool operator");
    }
  }

  private static Boolean calCompare(Row row, OperatorType type, List<Expression> inputs) {
    Object left = implement(row, inputs.get(0));
    Object right = implement(row, inputs.get(1));
    if (left == null || right == null) {
      return null;
    }
    ColumnType dType = dominate(inputs.get(0).getOutType(), inputs.get(1).getOutType());
    final int cmp = ((Comparable) cast(dType, (Number) left)).compareTo((Comparable) cast(dType, (Number) right));
    switch (type) {
      case GT:
        return cmp > 0;
      case GE:
        return cmp >= 0;
      case LT:
        return cmp < 0;
      case LE:
        return cmp <= 0;
      case EQ:
        return cmp == 0;
      case NE:
        return cmp != 0;
      default:
        throw new UnsupportedOperationException("not support compare type");
    }
  }

  private static ColumnType dominate(ColumnType a, ColumnType b) {
    if (a.getNumber() > b.getNumber()) {
      return a;
    } else {
      return b;
    }
  }

  private static Number calculate(Row row, OperatorType type, List<Expression> inputExps) {
    List<Number> inputs = inputExps.stream().map(e -> (Number) implement(row, e)).collect(Collectors.toList());
    ColumnType calType = inputExps.stream().reduce(ColumnType.INT, (d, t) -> dominate(d, t.getOutType()),
        (t1, t2) -> dominate(t1, t2));
    for (Object in : inputs) {
      if (in == null) {
        return null;
      }
    }
    switch (calType) {
      case INT:
        return calInt(type, inputs);
      case LONG:
        return calLong(type, inputs);
      case FLOAT:
        return calFloat(type, inputs);
      case DOUBLE:
        return calDouble(type, inputs);
      default:
        throw new UnsupportedOperationException("not support column type");
    }
  }

  private static int calInt(OperatorType type, List<Number> inputs) {
    switch (type) {
      case PLUS:
        return inputs.get(0).intValue() + inputs.get(1).intValue();
      case MINUS:
        return inputs.get(0).intValue() - inputs.get(1).intValue();
      case TIMES:
        return inputs.get(0).intValue() * inputs.get(1).intValue();
      case DIVIDE:
        return inputs.get(0).intValue() / inputs.get(1).intValue();
      case MOD:
        return inputs.get(0).intValue() % inputs.get(1).intValue();
      case PLUS_PRE:
        return inputs.get(0).intValue();
      case MINUS_PRE:
        return -inputs.get(0).intValue();
      default:
        throw new UnsupportedOperationException("not support op type");
    }
  }

  private static long calLong(OperatorType type, List<Number> inputs) {
    switch (type) {
      case PLUS:
        return inputs.get(0).longValue() + inputs.get(1).longValue();
      case MINUS:
        return inputs.get(0).longValue() - inputs.get(1).longValue();
      case TIMES:
        return inputs.get(0).longValue() * inputs.get(1).longValue();
      case DIVIDE:
        return inputs.get(0).longValue() / inputs.get(1).longValue();
      case MOD:
        return inputs.get(0).longValue() % inputs.get(1).longValue();
      case PLUS_PRE:
        return inputs.get(0).longValue();
      case MINUS_PRE:
        return -inputs.get(0).longValue();
      default:
        throw new UnsupportedOperationException("not support op type");
    }
  }

  private static float calFloat(OperatorType type, List<Number> inputs) {
    switch (type) {
      case PLUS:
        return inputs.get(0).floatValue() + inputs.get(1).floatValue();
      case MINUS:
        return inputs.get(0).floatValue() - inputs.get(1).floatValue();
      case TIMES:
        return inputs.get(0).floatValue() * inputs.get(1).floatValue();
      case DIVIDE:
        return inputs.get(0).floatValue() / inputs.get(1).floatValue();
      case MOD:
        return inputs.get(0).floatValue() % inputs.get(1).floatValue();
      case PLUS_PRE:
        return inputs.get(0).floatValue();
      case MINUS_PRE:
        return -inputs.get(0).floatValue();
      default:
        throw new UnsupportedOperationException("not support op type");
    }
  }

  private static double calDouble(OperatorType type, List<Number> inputs) {
    switch (type) {
      case PLUS:
        return inputs.get(0).doubleValue() + inputs.get(1).doubleValue();
      case MINUS:
        return inputs.get(0).doubleValue() - inputs.get(1).doubleValue();
      case TIMES:
        return inputs.get(0).doubleValue() * inputs.get(1).doubleValue();
      case DIVIDE:
        return inputs.get(0).doubleValue() / inputs.get(1).doubleValue();
      case MOD:
        return inputs.get(0).doubleValue() % inputs.get(1).doubleValue();
      case PLUS_PRE:
        return inputs.get(0).doubleValue();
      case MINUS_PRE:
        return -inputs.get(0).doubleValue();
      default:
        throw new UnsupportedOperationException("not support op type");
    }
  }

  public static Object cast(ColumnType type, Number value) {
    if (value == null) {
      return null;
    }
    // todo: support cast more types
    switch (type) {
      case INT:
        return value.intValue();
      case LONG:
        return value.longValue();
      case FLOAT:
        return value.floatValue();
      case DOUBLE:
        return value.doubleValue();
      default:
        throw new UnsupportedOperationException("not support op type");
    }
  }
}
