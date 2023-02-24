package com.hufudb.openhufu.data.schema;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnDesc;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuData.SchemaProto;

/**
 * Schema of relation table, used in @DataSet.java, @TableSchema.java.
 * Encapsulation of protocol buffer, immutable
 */
public class Schema {
  public final static Schema EMPTY = new Schema(SchemaProto.newBuilder().build());

  private final SchemaProto schema;

  Schema(SchemaProto proto) {
    this.schema = proto;
  }

  Schema(List<ColumnDesc> columns) {
    this.schema = SchemaProto.newBuilder().addAllColumnDesc(columns).build();
  }

  public static Schema fromProto(SchemaProto proto) {
    return new Schema(proto);
  }

  public static Schema fromColumnDesc(List<ColumnDesc> descs) {
    return new Schema(SchemaProto.newBuilder().addAllColumnDesc(descs).build());
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

  public ColumnDesc getColumnDesc(int id) {
    return schema.getColumnDesc(id);
  }

  public List<ColumnDesc> getColumnDescs() {
    return schema.getColumnDescList();
  }

  public SchemaProto toProto() {
    return schema;
  }

  public int size() {
    return schema.getColumnDescCount();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return String.join("|",
        schema
            .getColumnDescList().stream().map(col -> String.format("%s:%s:%s", col.getName(),
                col.getType(), col.getModifier()))
            .collect(Collectors.toList()));
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this
        || (obj instanceof Schema &&
            schema.equals(((Schema) obj).schema));
  }

  public static Schema merge(Schema left, Schema right) {
    SchemaProto.Builder builder = left.toProto().toBuilder();
    builder.addAllColumnDesc(right.getColumnDescs());
    return new Schema(builder.build());
  }

  public static class Builder {
    private final SchemaProto.Builder builder;

    Builder() {
      builder = SchemaProto.newBuilder();
    }

    public Builder add(String name, ColumnType type) {
      builder.addColumnDesc(ColumnDesc.newBuilder().setName(name).setType(type));
      return this;
    }

    public Builder add(String name, ColumnType type, Modifier modifier) {
      builder
          .addColumnDesc(ColumnDesc.newBuilder().setName(name).setType(type).setModifier(modifier));
      return this;
    }

    public Builder add(ColumnDesc columnDesc) {
      builder.addColumnDesc(columnDesc);
      return this;
    }

    public Builder merge(Schema schema) {
      builder.addAllColumnDesc(schema.toProto().getColumnDescList());
      return this;
    }

    public Schema build() {
      return new Schema(builder.build());
    }

    public int size() {
      return builder.getColumnDescCount();
    }
  }
}
