package group.bda.federate.sql.enumerator;


import java.util.Iterator;

import org.apache.calcite.linq4j.Enumerator;

import group.bda.federate.data.DataSet;
import group.bda.federate.data.Row;
import group.bda.federate.rpc.FederateCommon.DataSetProto;

public class RowEnumerator implements Enumerator<Row> {

  private final Enumerator<Row> enumerator;
  private int cnt;
  private int limitCount;

  public RowEnumerator(DataSet dataSet, int limitCount) {
    enumerator = new BatchEnumerator(dataSet);
    this.limitCount = limitCount;
  }

  public RowEnumerator(StreamingIterator<DataSetProto> iter, int limitCount) {
    enumerator = new StreamEnumerator(iter);
    this.limitCount = limitCount;
  }

  private RowEnumerator() {
    enumerator = new BatchEnumerator(DataSet.newBuilder(0).build());
  }


  public static RowEnumerator emptyEnumerator() {
    return new RowEnumerator();
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

  static class BatchEnumerator implements Enumerator<Row> {

    final DataSet dataSet;
    Iterator<Row> iterator;
    Row current;

    BatchEnumerator(DataSet dataSet) {
      this.dataSet = dataSet;
      this.iterator = dataSet.rawIterator();
    }

    @Override
    public Row current() {
      return current;
    }

    @Override
    public boolean moveNext() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      return false;
    }

    @Override
    public void reset() {
      iterator = dataSet.rawIterator();
      current = null;
    }

    @Override
    public void close() {
      // do nothing
    }
  }

  static public class StreamEnumerator implements Enumerator<Row> {
    StreamingIterator<DataSetProto> iter;
    DataSet backup;
    Row current;
    Iterator<Row> currentIter;

    public StreamEnumerator(StreamingIterator<DataSetProto> iter) {
      this.iter = iter;
      currentIter = new Iterator<Row>() {
        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Row next() {
          return null;
        }
      };
    }

    @Override
    public Row current() {
      return current;
    }

    @Override
    public boolean moveNext() {
      if (currentIter.hasNext()) {
        current = currentIter.next();
        return true;
      } else if (iter.hasNext()) {
        DataSet currentDataSet = DataSet.fromProto(iter.next());
        currentIter = currentDataSet.rawIterator();
        if (backup == null) {
          backup = currentDataSet;
        } else {
          backup.mergeDataSetUnsafe(currentDataSet);
        }
        return moveNext();
      }
      return false;
    }

    @Override
    public void reset() {
      current = null;
      currentIter = backup.rawIterator();
    }

    @Override
    public void close() {
      // do nothing
    }
  }
}