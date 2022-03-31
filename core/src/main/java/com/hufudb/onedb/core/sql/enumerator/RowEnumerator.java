package com.hufudb.onedb.core.sql.enumerator;

import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.StreamBuffer;
import com.hufudb.onedb.core.utils.EmptyEnumerator;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import org.apache.calcite.linq4j.Enumerator;

public class RowEnumerator implements Enumerator<Row> {

  private final Enumerator<Row> enumerator;
  private int cnt;
  private int limitCount;

  public RowEnumerator(StreamBuffer<DataSetProto> iter, int limitCount) {
    enumerator = new StreamEnumerator(iter);
    if (limitCount == 0) {
      this.limitCount = Integer.MAX_VALUE;
    } else {
      this.limitCount = limitCount;
    }
  }

  @Override
  public Row current() {
    return enumerator.current();
  }

  @Override
  public boolean moveNext() {
    cnt++;
    if (cnt > this.limitCount) {
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
    enumerator.close();
  }

  public static class StreamEnumerator implements Enumerator<Row> {
    StreamBuffer<DataSetProto> iter;
    BasicDataSet backup;
    Row current;
    Enumerator<Row> cEnumerator;

    public StreamEnumerator(StreamBuffer<DataSetProto> iter) {
      this.iter = iter;
      cEnumerator = new EmptyEnumerator<>();
    }

    @Override
    public Row current() {
      return current;
    }

    @Override
    public boolean moveNext() {
      if (cEnumerator.moveNext()) {
        current = cEnumerator.current();
        return true;
      } else if (iter.hasNext()) {
        BasicDataSet currentDataSet = BasicDataSet.fromProto(iter.next());
        if (backup == null) {
          backup = currentDataSet;
        } else {
          backup.mergeDataSet(currentDataSet);
        }
        cEnumerator = currentDataSet;
        return moveNext();
      }
      return false;
    }

    @Override
    public void reset() {
      current = null;
      cEnumerator = backup;
    }

    @Override
    public void close() {
      // do nothing
    }
  }
}
