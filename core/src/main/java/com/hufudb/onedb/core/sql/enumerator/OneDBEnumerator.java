package com.hufudb.onedb.core.sql.enumerator;

import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.sql.rel.OneDBQueryContext;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;

import org.apache.calcite.linq4j.Enumerator;

public class OneDBEnumerator implements Enumerator<Object> {
  private final Enumerator<Row> enumerator;

  public OneDBEnumerator(OneDBSchema schema, long contextId) {
    OneDBClient client = schema.getClient();
    enumerator = client.oneDBQuery(contextId);
    OneDBQueryContext.deleteContext(contextId);
  }

  @Override
  public Object current() {
    Row current = enumerator.current();
    if (current.size() == 1) {
      return current.getObject(0);
    }
    Object[] row = current.copyValues();
    return row;
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
