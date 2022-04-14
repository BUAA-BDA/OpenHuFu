package com.hufudb.onedb.core.data;

public enum OneDBNull {
  INSTANCE{
    @Override
    public String toString() {
        return "NULL";
    }
  };
}
