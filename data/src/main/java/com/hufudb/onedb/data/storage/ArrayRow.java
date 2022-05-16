package com.hufudb.onedb.data.storage;

public class ArrayRow implements Row {
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
