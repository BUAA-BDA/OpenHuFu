package com.hufudb.openhufu.core.sql.enumerator;

import com.hufudb.openhufu.core.client.OpenHuFuClient;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaManager;
import com.hufudb.openhufu.data.storage.ArrayRow;
import com.hufudb.openhufu.data.storage.Row;
import com.hufudb.openhufu.plan.QueryPlanPool;
import org.apache.calcite.linq4j.Enumerator;

public class OpenHuFuEnumerator implements Enumerator<Object> {
  private final Enumerator<Row> enumerator;

  public OpenHuFuEnumerator(OpenHuFuSchemaManager schema, long planId) {
    OpenHuFuClient client = schema.getClient();
    enumerator = client.openHuFuQuery(planId);
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
