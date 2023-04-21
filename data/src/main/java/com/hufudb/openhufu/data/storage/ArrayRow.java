package com.hufudb.openhufu.data.storage;

import com.hufudb.openhufu.data.storage.utils.CompareUtils;

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
        if (!CompareUtils.equalTo(get(i), r.get(i))) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "ArrayRow{" +
            "values=" + Arrays.toString(values) +
            '}';
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

  public static ArrayRow merge(Row row1, Row row2, int pass) {
    final int size1 = (pass == -1)? row1.size() : row1.size() - 1;
    final int size2 = row2.size();
    Builder builder = new Builder(size1 + size2);
    int j = 0;
    for (int i = 0; i < size1; ++i) {
      if (pass == i) {
        j++;
      }
      builder.set(i, row1.get(j));
      j++;
    }
    for (int i = 0; i < size2; ++i) {
      builder.set(i + size1, row2.get(i));
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
