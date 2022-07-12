package com.hufudb.onedb.data.storage;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.utils.DateUtils;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;

/**
 * Dataset which encapsulate a @java.sql.ResultSet
 */
public class ResultDataSet implements DataSet {
  final protected Schema schema;
  final protected ResultSet result;
  final protected DateUtils dateUtils;


  public ResultDataSet(Schema schema, ResultSet result) {
    this.schema = schema;
    this.result = result;
    this.dateUtils = new DateUtils();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  protected List<Getter> generateGetters() {
    ImmutableList.Builder<Getter> builder = ImmutableList.builder();
    int i = 1;
    for (ColumnDesc col : schema.getColumnDescs()) {
      final int idx = i;
      switch(col.getType()) {
        case BLOB:
          builder.add(() -> {
            return result.getBytes(idx);
          });
          break;
        case BOOLEAN:
          builder.add(() -> {
            return result.getBoolean(idx);
          });
          break;
        case BYTE:
        case SHORT:
        case INT:
          builder.add(() -> {
            return result.getInt(idx);
          });
          break;
        case DATE:
          builder.add(() -> {
            return result.getDate(idx);
          });
          break;
        case TIME:
          builder.add(() -> {
            return result.getTime(idx);
          });
          break;
        case TIMESTAMP:
          builder.add(() -> {
            return result.getTimestamp(idx);
          });
          break;
        case LONG:
          builder.add(() -> {
            return result.getLong(idx);
          });
          break;
        case STRING:
          builder.add(() -> {
            return result.getString(idx);
          });
          break;
        case DOUBLE:
          builder.add(() -> {
            return result.getDouble(idx);
          });
          break;
        case FLOAT:
          builder.add(() -> {
            return result.getFloat(idx);
          });
          break;
        default:
          break;
      }
      ++i;
    }
    return builder.build();
  }

  @Override
  public DataSetIterator getIterator() {
    return new ResultIterator(generateGetters());
  }

  @Override
  public void close() {
    try {
      result.close();
    } catch (SQLException e) {
      LOG.error("Fail to close ResultSet in ResultDatSet: {}", e.getMessage());
    }
  }

  public interface Getter<T> {
    T get() throws SQLException;
  }

  protected class ResultIterator implements DataSetIterator {
    List<Getter> getters;

    protected ResultIterator(List<Getter> getters) {
      this.getters = getters;
    }

    @Override
    public boolean next() {
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
        return getters.get(columnIndex).get();
      } catch (Exception e) {
        LOG.error("Error in get of ResultDataSet: {}", e.getMessage());
        return null;
      }
    }

    @Override
    public int size() {
      return schema.size();
    }
  }
}
