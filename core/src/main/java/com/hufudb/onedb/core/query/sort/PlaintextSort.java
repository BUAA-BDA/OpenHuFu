package com.hufudb.onedb.core.query.sort;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.query.QueryableDataSet;
import java.util.List;

public class PlaintextSort {
  public static QueryableDataSet apply(QueryableDataSet input, List<String> orders) {
    List<Row> rows = input.getRows();
    Header header = input.getHeader();
    rows.sort((o1, o2) -> {
      for (String str : orders) {
        OneDBOrder order = OneDBOrder.parse(str);
        int compareResult;
        switch (header.getType(order.idx)) {
          case INT:
          case LONG:
          case SHORT:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case DATE:
          case TIME:
          case TIMESTAMP:
            //TODO:
            compareResult = ((Comparable) o1.getObject(order.idx)).compareTo(o2.getObject(order.idx));
            if (order.direction.equals(OneDBOrder.Direction.DESC)) {
              compareResult = compareResult * -1;
            }
            break;
          default:
            throw new RuntimeException("the field can not be sorted");
        }
        if (compareResult != 0) {
          return compareResult;
        }
      }
      return 0;
    });
    return input;
  }
}
