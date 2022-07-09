package com.hufudb.onedb.owner.adapter.postgis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import com.hufudb.onedb.data.storage.utils.DateUtils;
import org.postgresql.util.PGobject;

/**
 * Dataset which encapsulate a @java.sql.ResultSet
 */
public class PostgisResultDataSet implements DataSet {
  final Schema schema;
  final ResultSet result;
  final DateUtils dateUtils;

  public PostgisResultDataSet(Schema schema, ResultSet result) {
    this.schema = schema;
    this.result = result;
    this.dateUtils = new DateUtils();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void close() {
    try {
      result.close();
    } catch (SQLException e) {
      LOG.error("Fail to close ResultSet in PostgisResultDatSet: {}", e.getMessage());
    }
  }

  @Override
  public DataSetIterator getIterator() {
    return new PostgisResultIterator();
  }

  public interface Getter<T> {
    T get() throws SQLException;
  }

  class PostgisResultIterator implements DataSetIterator {
    List<Getter> getters;

    PostgisResultIterator() {
      ImmutableList.Builder<Getter> builder = ImmutableList.builder();
      int i = 1;
      PostgisUtils postgisUtils = new PostgisUtils();
      for (ColumnDesc col : schema.getColumnDescs()) {
        final int idx = i;
        switch (col.getType()) {
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
          case POINT:
            builder.add(() -> {
              return postgisUtils.fromPGPoint(((PGobject)(result.getObject(idx))));
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
