package com.hufudb.onedb.owner.implementor.aggregate;

import java.util.List;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OwnerAggregation {
  static final Logger LOG = LoggerFactory.getLogger(OwnerAggregation.class);

  public static QueryableDataSet apply(QueryableDataSet input, List<Integer> groups, List<OneDBExpression> aggs, List<FieldType> types) {
    if (!groups.isEmpty()) {
      LOG.warn("Not support 'group by' clause");
      throw new UnsupportedOperationException("Not support 'group by' clause");
    }
    for (OneDBExpression exp : aggs) {
      
    }
    return null;
  }
}
