package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.schema.Schema;

/**
 * Vertical combination of two @MaterializedDataSet
 */
public class VerticalDataSet implements MaterializedDataSet {
  final Schema schema;
  final MaterializedDataSet left;
  final MaterializedDataSet right;
  final int leftSize;

  VerticalDataSet(MaterializedDataSet left, MaterializedDataSet right) {
    this.schema = Schema.merge(left.getSchema(), right.getSchema());
    this.left = left;
    this.right = right;
    this.leftSize = left.getSchema().size();
  }

  public static VerticalDataSet create(MaterializedDataSet left, MaterializedDataSet right) {
    if (left.rowCount() != right.rowCount()) {
      LOG.warn("Unmatch size in vertical dataset left {}, right {}", left.rowCount(), right.rowCount());
      return null;
    }
    return new VerticalDataSet(left, right);
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
    // do nothing
  }

  @Override
  public Object get(int rowIndex, int columnIndex) {
    if (columnIndex < leftSize) {
      return left.get(rowIndex, columnIndex);
    } else {
      return right.get(rowIndex, columnIndex - leftSize);
    }
  }

  @Override
  public int rowCount() {
    return left.rowCount();
  }

  class Iterator implements DataSetIterator {
    DataSetIterator leftIt;
    DataSetIterator rightIt;

    Iterator() {
      leftIt = left.getIterator();
      rightIt = right.getIterator();
    }

    @Override
    public Object get(int columnIndex) {
      if (columnIndex < leftSize) {
        return leftIt.get(columnIndex);
      } else {
        return rightIt.get(columnIndex - leftSize);
      }
    }

    @Override
    public int size() {
      return schema.size();
    }

    @Override
    public boolean next() {
      return leftIt.next() && rightIt.next();
    }
  }
}
