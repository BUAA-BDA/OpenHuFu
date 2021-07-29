package group.bda.federate.driver.utils;

import java.util.List;

public class PrivacyPreserving {
  public static int calQ(int xi, final List<Integer> coeList, int value) {
    int length = coeList.size();
    int q = value;
    for (int i = 0; i < length; ++i) {
      q += Math.pow(xi, i + 1) * coeList.get(i);
    }
    // System.out.printf("cal q with x[%d] coeList[%s] const[%d], result [%d]\n",
    // xi, coeList.toString(), value, q);
    return q;
  }
}