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

  public static Builder newBuilder(int size) {
    return new Builder(size);
  }

  public static class Builder {
    Object[] values;

    private Builder(int size) {
      values = new Object[size];
    }
  
    public void set(int index, Object value) {
      values[index] = value;
    }
  
    public Row build() {
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
