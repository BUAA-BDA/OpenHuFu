package com.hufudb.onedb.data.schema;

import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.hufudb.onedb.data.OneDBData.ColumnDesc;
import com.hufudb.onedb.data.OneDBData.ColumnType;
import com.hufudb.onedb.data.OneDBData.Modifier;
import com.hufudb.onedb.data.OneDBData.SchemaProto;

public class Schema {
  private final SchemaProto schema;
  private final Map<String, Integer> columnIndex;

  Schema(SchemaProto proto) {
    this.schema = proto;
    ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
    for (int i = 0; i < proto.getColumnDescCount(); ++i) {
      builder.put(proto.getColumnDesc(i).getName(), i);
    }
    this.columnIndex = builder.build();
  }

  public static Schema fromProto(SchemaProto proto) {
    return new Schema(proto);
  }

  public String getName(int id) {
    return schema.getColumnDesc(id).getName();
  }

  public Modifier getModifier(int id) {
    return schema.getColumnDesc(id).getModifier();
  }

  public ColumnType getType(int id) {
    return schema.getColumnDesc(id).getType();
  }

  public ColumnDesc getDesc(String name) {
    return schema.getColumnDesc(columnIndex.get(name));
  }

  public SchemaProto toProto() {
    return schema;
  }
}
