package com.hufudb.openhufu.data.schema.utils;

import com.hufudb.openhufu.data.storage.utils.ColumnTypeWrapper;
import com.hufudb.openhufu.data.storage.utils.ModifierWrapper;
import java.util.List;
import java.util.stream.Collectors;
import com.google.gson.annotations.SerializedName;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnDesc;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;

public class PojoColumnDesc {
  public String name;
  public ColumnTypeWrapper type;
  public ModifierWrapper modifier;
  @SerializedName(value = "columnId", alternate = { "columnid", "column_id" })
  public int columnId;

  public PojoColumnDesc() {}

  public PojoColumnDesc(String name, ColumnTypeWrapper type, ModifierWrapper modifier, int columnId) {
    this.name = name;
    this.type = type;
    this.modifier = modifier;
    this.columnId = columnId;
  }

  public ColumnType getType() {
    return type.get();
  }

  public Modifier getModifier() {
    return modifier.get();
  }

  public int getColumnId() {
    return columnId;
  }

  public static PojoColumnDesc fromColumnDesc(ColumnDesc col) {
    PojoColumnDesc pcol = new PojoColumnDesc();
    pcol.name = col.getName();
    pcol.type = ColumnTypeWrapper.of(col.getType());
    pcol.modifier = ModifierWrapper.of(col.getModifier());
    return pcol;
  }

  public static List<PojoColumnDesc> fromColumnDesc(List<ColumnDesc> colums) {
    return colums.stream().map(col -> fromColumnDesc(col)).collect(Collectors.toList());
  }

  public ColumnDesc toColumnDesc() {
    return ColumnDesc.newBuilder().setName(name).setType(getType()).setModifier(getModifier())
        .build();
  }
}
