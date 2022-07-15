package com.hufudb.onedb.owner.adapter.json;

import com.hufudb.onedb.data.function.Mapper;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.data.storage.MapDataSet;
import com.hufudb.onedb.owner.adapter.json.jsonsrc.JsonSrc;
import com.hufudb.onedb.proto.OneDBData;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class JsonTable {
  final String tableName;
  final Schema schema;
  final JsonSrc jsonSrc;

  public JsonTable(String tableName, JsonSrc jsonSrc) {
    this.tableName = tableName;
    this.jsonSrc = jsonSrc;
    Schema.Builder builder = Schema.newBuilder();
    this.jsonSrc.getColumnsNames().forEach(col -> builder.add(col, OneDBData.ColumnType.STRING));
    this.schema = builder.build();
  }

  Schema getSchema() {
    return schema;
  }

  DataSet scanWithSchema(Schema outSchema, List<Integer> mapping) {
    List<Mapper> mappers = new ArrayList<>();
    for (int i = 0; i < mapping.size(); ++i) {
      final int actualColumnIdx = mapping.get(i);
      final OneDBData.ColumnType outType = outSchema.getType(i);
      switch (outType) {
        case BOOLEAN:
          mappers.add(row -> (row.get(actualColumnIdx) == null ? null : Boolean.valueOf((String) row.get(actualColumnIdx))));
          break;
        case BYTE:
        case SHORT:
        case INT:
          mappers.add(row -> (row.get(actualColumnIdx) == null ? null : Integer.valueOf((String) row.get(actualColumnIdx))));
          break;
        case LONG:
          mappers.add(row -> (row.get(actualColumnIdx) == null ? null : Long.valueOf((String) row.get(actualColumnIdx))));
          break;
        case TIMESTAMP:
          mappers.add(row -> (row.get(actualColumnIdx) == null ? null : Timestamp.valueOf((String) row.get(actualColumnIdx))));
          break;
        case DATE:
          mappers.add(row -> (row.get(actualColumnIdx) == null ? null : Date.valueOf((String) row.get(actualColumnIdx))));
          break;
        case TIME:
          mappers.add(row -> (row.get(actualColumnIdx) == null ? null : Time.valueOf((String) row.get(actualColumnIdx))));
          break;
        case FLOAT:
          mappers.add(row -> (row.get(actualColumnIdx) == null ? null : Float.valueOf((String) row.get(actualColumnIdx))));
          break;
        case DOUBLE:
          mappers.add(row -> (row.get(actualColumnIdx) == null ? null : Double.valueOf((String) row.get(actualColumnIdx))));
          break;
        case STRING:
          mappers.add(row -> (row.get(actualColumnIdx) == null ? null : row.get(actualColumnIdx)));
          break;
        default:
          throw new RuntimeException("Unsupported type for json adapter");
      }
    }
    try {
      return MapDataSet.create(outSchema, mappers, new JsonDateSet(this.schema, this.jsonSrc));
    } catch (Exception e) {
      return EmptyDataSet.INSTANCE;
    }
  }
}
