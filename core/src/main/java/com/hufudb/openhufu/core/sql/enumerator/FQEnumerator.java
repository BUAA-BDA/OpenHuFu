package com.hufudb.openhufu.core.sql.enumerator;

import com.hufudb.openhufu.core.client.FQClient;
import com.hufudb.openhufu.core.sql.schema.FQSchemaManager;
import com.hufudb.openhufu.data.storage.ArrayRow;
import com.hufudb.openhufu.data.storage.Row;
import com.hufudb.openhufu.plan.QueryPlanPool;
import org.apache.calcite.linq4j.Enumerator;

public class FQEnumerator implements Enumerator<Object> {
  private final Enumerator<Row> enumerator;

  public FQEnumerator(FQSchemaManager schema, long planId) {
    FQClient client = schema.getClient();
    enumerator = client.fqQuery(planId);
    QueryPlanPool.deletePlan(planId);
  }

  @Override
  public Object current() {
    Row current = enumerator.current();
    if (current.size() == 1) {
      return current.get(0);
    }
    return ArrayRow.materialize2ObjectArray(current);
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
