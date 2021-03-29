package tk.onedb.core.sql.rel;

import java.lang.reflect.Method;

import com.google.common.collect.ImmutableMap;

import org.apache.calcite.linq4j.tree.Types;

public enum OneDBMethod {
  ONEDB_TABLE_QUERY(OneDBTable.OneDBQueryable.class, "query");

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
