package com.hufudb.onedb.owner.adapter.postgis;

import java.sql.ResultSet;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.ResultDataSet;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import org.postgresql.util.PGobject;

/**
 * PostGIS extension of ResultDataSet
 */
public class PostgisResultDataSet extends ResultDataSet {

  public PostgisResultDataSet(Schema schema, ResultSet result) {
    super(schema, result);
  }

  @Override
  protected List<Getter> generateGetters() {
    ImmutableList.Builder<Getter> builder = ImmutableList.builder();
    int i = 1;
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
            return PostgisUtils.fromPGPoint(((PGobject) (result.getObject(idx))));
          });
          break;
        default:
          break;
      }
      ++i;
    }
    return builder.build();
  }
}
