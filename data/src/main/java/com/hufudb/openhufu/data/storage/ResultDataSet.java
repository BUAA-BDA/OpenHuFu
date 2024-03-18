package com.hufudb.openhufu.data.storage;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnDesc;

/**
 * Dataset which encapsulate a @java.sql.ResultSet
 */
public class ResultDataSet implements DataSet {
  final protected Schema schema;
  final protected ResultSet result;


  public ResultDataSet(Schema schema, ResultSet result) {
    this.schema = schema;
    this.result = result;
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
        case VECTOR:
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
        case GEOMETRY:
          builder.add(() -> {
            return result.getString(idx);
          });
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
