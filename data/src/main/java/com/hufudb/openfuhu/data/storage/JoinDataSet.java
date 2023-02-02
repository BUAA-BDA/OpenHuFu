package com.hufudb.openhufu.data.storage;

import com.hufudb.openhufu.data.function.Matcher;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinType;
import java.util.HashSet;
import java.util.Set;

/**
 * A dataset used for join through nested loop
 * materialize left table before executing
 * perform better when the size of the left dataset is smaller than the right dataset
 */
public class JoinDataSet implements DataSet {
  final Schema schema;
  final MaterializedDataSet left;
  final DataSet right;
  final Matcher matcher;
  final int leftRowCount;
  final int leftSize;
  final JoinType joinType;
  enum RowStatus {
    UNMATCHED,
    MATCHED,
    LEFTNULL,
    RIGHTNULL
  };

  JoinDataSet(DataSet left, DataSet right, Matcher matcher, JoinType joinType) {
    this.schema = Schema.merge(left.getSchema(), right.getSchema());
    ProtoDataSet leftDataSet = ProtoDataSet.materialize(left);
    this.left = leftDataSet;
    this.leftRowCount = leftDataSet.rowCount();
    this.leftSize = left.getSchema().size();
    this.right = right;
    this.matcher = matcher;
    this.joinType = joinType;
  }

  public static JoinDataSet create(DataSet left, DataSet right, Matcher matcher, JoinType type) {
    return new JoinDataSet(left, right, matcher, type);
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

  public interface JoinNext {
    boolean next();
  }
  /**
    For left join, masterRow means leftRow(row of left table). Otherwise, it means rightRow.
    RowIndex and matchedRows are only used for outer join.
  */
  class Iterator implements DataSetIterator {

    DataSetIterator leftIter;
    DataSetIterator rightIter;
    MaterializedDataSet mright;
    Row masterRow;
    RowStatus status;
    int rowIndex;
    Set<Integer> matchedRows;
    JoinNext joinNext;

    public Iterator() {
      leftIter = left.getIterator();
      rightIter = right.getIterator();
      switch (joinType) {
        case SEMI://todo:support semi join
        case INNER:
          masterRow = rightIter.next() ? rightIter : null;
          this.joinNext = this::innerJoinNext;
          break;
        case LEFT:
          masterRow = leftIter.next() ? leftIter : null;
          mright = ProtoDataSet.materialize(right);
          rightIter = mright.getIterator();
          this.joinNext = this::leftJoinNext;
          break;
        case RIGHT:
          masterRow = rightIter.next() ? rightIter : null;
          this.joinNext = this::rightJoinNext;
          break;
        case OUTER:
          masterRow = rightIter.next() ? rightIter : null;
          matchedRows = new HashSet<>();
          this.joinNext = this::outerJoinNext;
          break;
        default:
          throw new UnsupportedOperationException("Unsupported type for join");
      }
      status = RowStatus.UNMATCHED;
      rowIndex = -1;
    }

    @Override
    public Object get(int columnIndex) {
      if (columnIndex < leftSize) {
        if (status == RowStatus.LEFTNULL) return null;
        else return leftIter.get(columnIndex);
      } else {
        if (status == RowStatus.RIGHTNULL) return null;
        else return rightIter.get(columnIndex - leftSize);
      }
    }

    @Override
    public int size() {
      return schema.size();
    }

    @Override
    public boolean next() {
      return this.joinNext.next();
    }

    private boolean innerJoinNext() {
      while (masterRow != null) {
        while (leftIter.next()) {
          if (matcher.match(leftIter, masterRow)) {
            return true;
          }
        }
        masterRow = rightIter.next() ? rightIter : null;
        leftIter = left.getIterator();
      }
      return false;
    }

    private boolean leftJoinNext() {
      if (status == RowStatus.RIGHTNULL) {
        masterRow = leftIter.next() ? leftIter : null;
        rightIter = mright.getIterator();
        status = RowStatus.UNMATCHED;
      }
      while (masterRow != null) {
        while (rightIter.next()) {
          if (matcher.match(masterRow, rightIter)) {
            status = RowStatus.MATCHED;
            return true;
          }
        }
        if (status == RowStatus.UNMATCHED) {
          status = RowStatus.RIGHTNULL;
          return true;
        }else {
          masterRow = leftIter.next() ? leftIter : null;
          rightIter = mright.getIterator();
          status = RowStatus.UNMATCHED;
        }
      }
      return false;
    }

    private boolean rightJoinNext() {
      if (status == RowStatus.LEFTNULL) {
        masterRow = rightIter.next() ? rightIter : null;
        leftIter = left.getIterator();
        status = RowStatus.UNMATCHED;
      }
      while (masterRow != null) {
        while (leftIter.next()) {
          if (matcher.match(leftIter, masterRow)) {
            status = RowStatus.MATCHED;
            return true;
          }
        }
        if (status == RowStatus.UNMATCHED) {
          status = RowStatus.LEFTNULL;
          return true;
        }else {
          masterRow = rightIter.next() ? rightIter : null;
          leftIter = left.getIterator();
          status = RowStatus.UNMATCHED;
        }
      }
      return false;
    }

    private boolean outerJoinNext() {
      if (status == RowStatus.RIGHTNULL) {
        while(leftIter.next()) {
          rowIndex++;
          if (!matchedRows.contains(rowIndex)) return true;
        }
        return false;
      }
      if (status == RowStatus.LEFTNULL) {
        masterRow = rightIter.next() ? rightIter : null;
        leftIter = left.getIterator();
        rowIndex = -1;
        status = RowStatus.UNMATCHED;
      }
      while (masterRow != null) {
        while (leftIter.next()) {
          rowIndex++;
          if (matcher.match(leftIter, masterRow)) {
            status = RowStatus.MATCHED;
            matchedRows.add(rowIndex);
            return true;
          }
        }
        if (status == RowStatus.UNMATCHED) {
          status = RowStatus.LEFTNULL;
          return true;
        }else {
          masterRow = rightIter.next() ? rightIter : null;
          leftIter = left.getIterator();
          rowIndex = -1;
          status = RowStatus.UNMATCHED;
        }
      }
      if (matchedRows.size() < leftRowCount) {
        status = RowStatus.RIGHTNULL;
        leftIter = left.getIterator();
        rowIndex = -1;
        while(leftIter.next()) {
          rowIndex++;
          if (!matchedRows.contains(rowIndex)) return true;
        }
      }
      return false;
    }
  }
}
