package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.function.Matcher;
import com.hufudb.onedb.data.schema.Schema;

/**
 * A dataset used for join through nested loop
 * materialize left table before executing
 * perform better when the size of the left dataset is smaller than the right dataset
 */
public class JoinDataSet implements DataSet {
  final Schema schema;
  final DataSet left;
  final DataSet right;
  final Matcher matcher;
  final int leftRowCount;
  final int leftSize;

  JoinDataSet(DataSet left, DataSet right, Matcher matcher) {
    this.schema = Schema.merge(left.getSchema(), right.getSchema());
    ProtoDataSet leftDataSet = ProtoDataSet.materalize(left);
    this.left = leftDataSet;
    this.leftRowCount = leftDataSet.rowCount();
    this.leftSize = left.getSchema().size();
    this.right = right;
    this.matcher = matcher;
  }

  public static JoinDataSet create(DataSet left, DataSet right, Matcher matcher) {
    return new JoinDataSet(left, right, matcher);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new Iterator();
  }

  @Override
  public void close() {
    left.close();
    right.close();
  }

  class Iterator implements DataSetIterator {
    DataSetIterator leftIter;
    DataSetIterator rightIter;
    Row rightRow;

    public Iterator() {
      leftIter = left.getIterator();
      rightIter = right.getIterator();
      rightRow = rightIter.next() ? rightIter : null;
    }

    @Override
    public Object get(int columnIndex) {
      if (columnIndex < leftSize) {
        return leftIter.get(columnIndex);
      } else {
        return rightRow.get(columnIndex - leftSize);
      }
    }

    @Override
    public int size() {
      return schema.size();
    }

    @Override
    public boolean next() {
      while (rightRow != null) {
        while (leftIter.next()) {
          if (matcher.match(leftIter, rightRow)) {
            return true;
          }
        }
        rightRow = rightIter.next() ? rightIter : null;
        leftIter = left.getIterator();
      }
      return false;
    }
  }
}
