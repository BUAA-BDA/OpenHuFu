package group.bda.federate.security.union;

import java.util.List;

import group.bda.federate.data.DataSet;
import group.bda.federate.data.Header;
import group.bda.federate.data.RandomDataSet;
import group.bda.federate.data.Row;

public class RandomSharesSetUnion {
  public static RandomDataSet generateRandomSet(DataSet in) {
    return new RandomDataSet(in);
  }

  public static RandomDataSet generateRandomSet(Header header, List<Row> rows) {
    return new RandomDataSet(DataSet.newDataSetUnsafe(header, rows));
  }

  public static DataSet removeRandomSet(DataSet in, RandomDataSet r) {
    return r.removeRandom(in);
  }
}
