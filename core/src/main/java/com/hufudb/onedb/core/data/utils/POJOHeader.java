package com.hufudb.onedb.core.data.utils;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Field;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import java.util.ArrayList;
import java.util.List;

public class POJOHeader {
  public List<Field> fields;

  public POJOHeader(List<Field> fields) {
    this.fields = fields;
  }

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  public static POJOHeader fromHeader(Header header) {
    return new POJOHeader(ImmutableList.copyOf(header.getFields()));
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public String toString() {
    List<String> columnStr = new ArrayList<>();
    for (Field field : fields) {
      columnStr.add(field.toString());
    }
    return String.join(" | ", columnStr);
  }

  public static class Builder {
    private List<Field> fields;

    private Builder() {
      fields = new ArrayList<Field>();
    }

    public Builder add(String name, FieldType type) {
      fields.add(Field.of(name, type));
      return this;
    }

    public Builder add(String name, FieldType type, Level level) {
      fields.add(Field.of(name, type, level));
      return this;
    }

    public Builder add(Field field) {
      fields.add(field);
      return this;
    }

    public POJOHeader build() {
      return new POJOHeader(fields);
    }

    public int size() {
      return fields.size();
    }
  }
}
