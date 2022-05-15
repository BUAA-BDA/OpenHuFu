package com.hufudb.onedb.core.sql.enumerator;

import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.data.storage.ArrayRow;
import com.hufudb.onedb.data.storage.Row;
import com.hufudb.onedb.plan.QueryPlanPool;
import org.apache.calcite.linq4j.Enumerator;

public class OneDBEnumerator implements Enumerator<Object> {
  private final Enumerator<Row> enumerator;

  public OneDBEnumerator(OneDBSchema schema, long planId) {
    OneDBClient client = schema.getClient();
    enumerator = client.oneDBQuery(planId);
    QueryPlanPool.deletePlan(planId);
  }

  @Override
  public Object current() {
    Row current = enumerator.current();
    if (current.size() == 1) {
      return current.get(0);
    }
    return ArrayRow.materialize(current);
  }

  @Override
  public boolean moveNext() {
    return enumerator.moveNext();
  }

  @Override
  public void reset() {
    enumerator.reset();
  }

  @Override
  public void close() {
    // do nothing
  }
}
