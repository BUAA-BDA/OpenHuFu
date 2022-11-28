package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.storage.utils.CompareUtils;

import java.io.Serializable;
import java.util.Arrays;

public class ArrayRow implements Row, Serializable {
  private static final long serialVersionUID = 12345L;

  final Object values[];

  ArrayRow(Object[] values) {
    this.values = values;
  }

  public void set(int idx, Object v) {
    values[idx] = v;
  }

  @Override
  public Object get(int columnIndex) {
    return values[columnIndex];
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj instanceof Row) {
      Row r = (Row) obj;
      int size = r.size();
      if (size != values.length) {
        return false;
      }
      for (int i = 0; i < size; ++i) {
        if (!CompareUtils.equal(get(i), r.get(i))) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  public static Builder newBuilder(int size) {
    return new Builder(size);
  }

  public static ArrayRow materialize(Row row) {
    final int size = row.size();
    Builder builder = new Builder(size);
    for (int i = 0; i < size; ++i) {
      builder.set(i, row.get(i));
    }
    return builder.build();
  }

  public static Object[] materialize2ObjectArray(Row row) {
    final int size = row.size();
    Builder builder = new Builder(size);
    for (int i = 0; i < size; ++i) {
      builder.set(i, row.get(i));
    }
    return builder.values;
  }

  public static class Builder {
    Object[] values;

    private Builder(int size) {
      values = new Object[size];
    }

    public void set(int index, Object value) {
      values[index] = value;
    }

    public ArrayRow build() {
      return new ArrayRow(values);
    }

    public void reset() {
      values = new Object[values.length];
    }

    public int size() {
      return values.length;
    }
  }
}
