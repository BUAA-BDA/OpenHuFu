package com.hufudb.onedb.core.sql.rel;

import java.lang.reflect.Method;

import com.google.common.collect.ImmutableMap;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;

import org.apache.calcite.linq4j.tree.Types;

public enum OneDBMethod {
  ONEDB_TABLE_QUERY(OneDBTable.OneDBQueryable.class, "query", String.class),
  ONEDB_SCHEMA_TEST(OneDBSchema.class, "query", String.class);

  public final Method method;

  public static final ImmutableMap<Method, OneDBMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, OneDBMethod> builder = ImmutableMap.builder();
    for (OneDBMethod value : OneDBMethod.values()) {
      builder.put(value.method, value);
    }
    MAP = builder.build();
  }

  OneDBMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }
}
