package group.bda.federate.security.secretsharing.utils;

import java.util.List;
import java.util.Vector;

public class ShamirCache {
  private final List<Double> qValueList;

  public ShamirCache() {
    qValueList = new Vector<>();
  }

  public void setQValue(double qValue) {
    qValueList.add(qValue);
  }

  public double calSum() {
    long sum = 0;
    for (double q : qValueList) {
      sum += q;
    }
    return sum;
  }

  public int getSize() {
    return qValueList.size();
  }
}
