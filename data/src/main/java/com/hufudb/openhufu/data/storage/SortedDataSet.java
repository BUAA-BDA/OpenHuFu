package com.hufudb.openhufu.data.storage;

import java.util.List;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.utils.CompareUtils;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Collation;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Direction;

public class SortedDataSet implements MaterializedDataSet {
  private final Schema schema;
  private final DataSet source;
  private final List<Collation> collations;
  private final ArrayDataSet output;

  SortedDataSet(DataSet source, List<Collation> collations) {
    this.schema = source.getSchema();
    this.source = source;
    this.collations = collations;
    this.output = ArrayDataSet.materialize(source);
    sort();
  }

  public static DataSet sort(DataSet source, List<Collation> collation) {
    if (collation.isEmpty()) {
      return source;
    }
    return new SortedDataSet(source, collation);
  }

  private void sort() {
    output.rows.sort((o1, o2) -> compare(o1, o2));
  }

  private int compare(Row r1, Row r2) {
    for (Collation coll : collations) {
      int compareResult = 0;
      switch (schema.getType(coll.getRef())) {
        case INT:
        case LONG:
        case SHORT:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case DATE:
        case TIME:
        case TIMESTAMP:
          // TODO:
          compareResult = CompareUtils.compare(r1.get(coll.getRef()),r2.get(coll.getRef()));
          if (coll.getDirection().equals(Direction.DESC)) {
            compareResult = -compareResult;
          }
          break;
        default:
          LOG.error("Unsupported sort key type {}", schema);
          throw new UnsupportedOperationException("the field can not be sorted");
      }
      if (compareResult != 0) {
        return compareResult;
      }
    }
    return 0;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return output.getIterator();
  }

  @Override
  public void close() {
    source.close();
  }

  @Override
  public Object get(int rowIndex, int columnIndex) {
    return output.get(rowIndex, columnIndex);
  }

  @Override
  public int rowCount() {
    return output.rowCount();
  }
}
