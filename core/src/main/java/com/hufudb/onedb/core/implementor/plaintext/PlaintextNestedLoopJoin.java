package com.hufudb.onedb.core.implementor.plaintext;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import java.util.List;

public class PlaintextNestedLoopJoin {

  public static QueryableDataSet apply(
      QueryableDataSet left, QueryableDataSet right, OneDBJoinInfo joinInfo) {
    List<Integer> leftKeys = joinInfo.getLeftKeys();
    List<Integer> rightKeys = joinInfo.getRightKeys();
    List<Row> leftData = left.getRows();
    List<Row> rigthData = right.getRows();
    List<OneDBExpression> filters = joinInfo.getConditions();
    final boolean hasThetaJoin = filters.size() > 0;
    Header outputHeader = Header.joinHeader(left.getHeader(), right.getHeader());
    QueryableDataSet result = PlaintextQueryableDataSet.fromHeader(outputHeader);
    for (Row lr : leftData) {
      for (Row rr : rigthData) {
        // todo: put the join condition predication into another function
        boolean join = true;
        for (int i = 0; i < leftKeys.size(); ++i) {
          int lk = leftKeys.get(i);
          int rk = rightKeys.get(i);
          if (!lr.getObject(lk).equals(rr.getObject(rk))) {
            join = false;
            break;
          }
        }
        if (!join) {
          continue;
        }
        Row row = Row.merge(lr, rr);
        if (!hasThetaJoin) {
          result.addRow(row);
        } else if (PlaintextFilter.filterRow(row, filters)) {
          result.addRow(row);
        }
      }
    }
    return result;
  }
}
