package com.hufudb.openhufu.data.schema.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnDesc;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;

public class PojoSchema {
  public List<PojoColumnDesc> columns;

  public static PojoSchema fromColumns(List<ColumnDesc> descs) {
    PojoSchema pschema = new PojoSchema();
    pschema.columns = descs.stream().map(
      col -> PojoColumnDesc.fromColumnDesc(col)
    ).collect(Collectors.toList());
    return pschema;
  }

  public static PojoSchema fromSchema(Schema schema) {
    return fromColumns(schema.getColumnDescs());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final List<ColumnDesc> ColumnTypes;

    private Builder() {
      ColumnTypes = new ArrayList<ColumnDesc>();
    }

    public Builder add(String name, ColumnType type) {
      ColumnTypes.add(ColumnDesc.newBuilder().setName(name).setType(type).setModifier(Modifier.PUBLIC).build());
      return this;
    }

    public Builder add(String name, ColumnType type, Modifier modifier) {
      ColumnTypes.add(ColumnDesc.newBuilder().setName(name).setType(type).setModifier(modifier).build());
      return this;
    }

    public Builder add(ColumnDesc desc) {
      ColumnTypes.add(desc);
      return this;
    }

    public PojoSchema build() {
      return fromColumns(ColumnTypes);
    }

    public int size() {
      return ColumnTypes.size();
    }
  }
}
