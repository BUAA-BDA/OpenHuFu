package com.hufudb.onedb.data.storage;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;

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
    return new ResultIterator();
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
    List<Getter> getters;

    ResultIterator() {
      ImmutableList.Builder<Getter> builder = ImmutableList.builder();
      int i = 1;
      for (ColumnDesc col : schema.getColumnDescs()) {
        final int idx = i;
        switch(col.getType()) {
          case BLOB:
            builder.add(() -> {
              try {
                return result.getBytes(idx);
              } catch (SQLException e) {
                throw new RuntimeException("Error in resultDataSet");
              }
            });
            break;
          case BOOLEAN:
            builder.add(() -> {
              try {
                return result.getBoolean(idx);
              } catch (SQLException e) {
                throw new RuntimeException("Error in resultDataSet");
              }
            });
            break;
          case BYTE:
          case SHORT:
          case INT:
            builder.add(() -> {
              try {
                return result.getInt(idx);
              } catch (SQLException e) {
                throw new RuntimeException("Error in resultDataSet");
              }
            });
            break;
          case DATE:
          case TIME:
          case TIMESTAMP:
          case LONG:
            builder.add(() -> {
              try {
                return result.getLong(idx);
              } catch (SQLException e) {
                throw new RuntimeException("Error in resultDataSet");
              }
            });
            break;
          case STRING:
            builder.add(() -> {
              try {
                return result.getString(idx);
              } catch (SQLException e) {
                throw new RuntimeException("Error in resultDataSet");
              }
            });
            break;
          case DOUBLE:
            builder.add(() -> {
              try {
                return result.getDouble(idx);
              } catch (SQLException e) {
                throw new RuntimeException("Error in resultDataSet");
              }
            });
            break;
          case FLOAT:
            builder.add(() -> {
              try {
                return result.getFloat(idx);
              } catch (SQLException e) {
                throw new RuntimeException("Error in resultDataSet");
              }
            });
            break;
          default:
            break;
        }
        ++i;
      }
      this.getters = builder.build();
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
      } catch (RuntimeException e) {
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
