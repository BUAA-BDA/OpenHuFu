package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.function.Matcher;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.proto.OneDBPlan.JoinType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

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

    public Iterator() {
      leftIter = left.getIterator();
      rightIter = right.getIterator();
      if (joinType == JoinType.LEFT) {
        masterRow = leftIter.next() ? leftIter : null;
        ProtoDataSet materializeRight = ProtoDataSet.materialize(right);
        this.mright = materializeRight;
        rightIter = mright.getIterator();
      }else {
        masterRow = rightIter.next() ? rightIter : null;
      }
      status = RowStatus.UNMATCHED;
      rowIndex = -1;
      matchedRows = joinType == JoinType.OUTER ? new HashSet<>() : null;
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
      switch (joinType) {
        case SEMI:
          //todo:suport semi join
        case INNER:
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
        case LEFT:
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
        case RIGHT:
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
        case OUTER:
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
        default:
          //invalid
          return false;
      }
    }
  }
}
