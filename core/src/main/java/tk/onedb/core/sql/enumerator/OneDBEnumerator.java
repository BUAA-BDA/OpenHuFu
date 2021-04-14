package tk.onedb.core.sql.enumerator;

import org.apache.calcite.linq4j.Enumerator;

import tk.onedb.core.client.OneDBClient;
import tk.onedb.core.data.Row;
import tk.onedb.core.sql.schema.OneDBSchema;
import tk.onedb.rpc.OneDBCommon.OneDBQueryProto;

public class OneDBEnumerator implements Enumerator<Object> {
  private final Enumerator<Row> enumerator;
  private final int limitCount;
  private int cnt = 0;

  public OneDBEnumerator(String tableName, OneDBSchema schema, OneDBQueryProto query) {
    if (query.getFetch() == 0) {
      this.limitCount = Integer.MAX_VALUE;
    } else {
      this.limitCount = query.getFetch();
    }
    OneDBClient client = schema.getClient();
    enumerator = client.oneDBQuery(tableName, query);
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
    cnt++;
    if (cnt > this.limitCount && this.limitCount >= 0) {
      return false;
    }
    return enumerator.moveNext();
  }

  @Override
  public void reset() {
    cnt = 0;
    enumerator.reset();
  }

  @Override
  public void close() {
    // do nothing
  }
}
