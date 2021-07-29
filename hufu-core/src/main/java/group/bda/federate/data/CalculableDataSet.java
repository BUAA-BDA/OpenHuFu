package group.bda.federate.data;

import java.util.List;

import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.sql.type.FederateFieldType;

public class CalculableDataSet {
  final DataSet in;
  final List<Expression> exps;
  DataSet out;

  public CalculableDataSet(DataSet in, List<Expression> exps) {
    this.in = in;
    this.exps = exps;
    DataSet.Builder builder = DataSet.newBuilder(exps.size());
    for (int i = 0; i < exps.size(); ++i) {
      Expression exp = exps.get(i);
      builder.set(i, "", FederateFieldType.values()[exp.getIr(exp.getIrCount() - 1).getOutType()]);
    }
    this.out = builder.build();
  }

  public void calculate() {
    for (Expression exp : exps) {
      List<IR> irs = exp.getIrList();
      for (IR ir : irs) {
        
      }
    }
  }

  public long plus(long a, long b) {
    return a + b;
  }

  public double plus(double a, double b) {
    return a + b;
  }

  public long minus(long a, long b) {
    return a - b;
  }

  public double minus(double a, double b) {
    return a - b;
  }

  public long times(long a, long b) {
    return a * b;
  }

  public double times(double a, double b) {
    return a * b;
  }

  public long divide(long a, long b) {
    return a / b;
  }

  public double divide(double a, double b) {
    return a / b;
  }

  public long mod(long a, long b) {
    return a % b;
  }

  public double mod(double a, double b) {
    return a % b;
  }

  public boolean gt(Comparable a, Comparable b) {
    return a.compareTo(b) > 0;
  }

  public boolean ge(Comparable a, Comparable b) {
    return a.compareTo(b) >= 0;
  }

  public boolean eq(Comparable a, Comparable b) {
    return a.compareTo(b) == 0;
  }

  public boolean le(Comparable a, Comparable b) {
    return a.compareTo(b) <= 0;
  }

  public boolean lt(Comparable a, Comparable b) {
    return a.compareTo(b) < 0;
  }

  public boolean ne(Comparable a, Comparable b) {
    return a.compareTo(b) != 0;
  }

  public boolean and(boolean a, boolean b) {
    return a && b;
  }

  public boolean or(boolean a, boolean b) {
    return a || b;
  }

  public boolean not(boolean a) {
    return !a;
  }

  public Object as(Object a) {
    return a;
  }
}
