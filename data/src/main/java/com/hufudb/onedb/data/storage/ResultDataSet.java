package com.hufudb.onedb.data.storage;

import java.sql.ResultSet;
import java.sql.SQLException;
import com.hufudb.onedb.data.schema.Schema;

/**
 * Dataset which encapsulate a @java.sql.ResultSet
 */
public class ResultDataSet implements DataSet {
  final Schema schema;
  final ResultSet result;

  public ResultDataSet(Schema schema, ResultSet result) {
    this.schema = schema;
    this.result = result;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return null;
  }

  @Override
  public void close() {
    try {
      result.close();
    } catch (SQLException e) {
      LOG.error("Fail to close ResultSet in ResultDatSet: {}", e.getMessage());
    }
  }

  class ResultIterator implements DataSetIterator {
    final ResultSet result;

    ResultIterator(ResultSet result) {
      this.result = result;
    }

    @Override
    public boolean hasNext() {
      try {
        return result.next();
      } catch (SQLException e) {
        LOG.error("Erorr in hasNext of ResultDataSet: {}", e.getMessage());
        return false;
      }
    }

    @Override
    public Object get(int columnIndex) {
      try {
        return result.getObject(columnIndex);
      } catch (SQLException e) {
        LOG.error("Error in get of ResultDataSet: {}", e.getMessage());
        return null;
      }
    }
  }
}
