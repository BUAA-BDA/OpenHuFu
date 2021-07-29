package group.bda.federate.sql.operator;

import java.lang.reflect.Method;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import group.bda.federate.sql.join.FedSpatialJoinInfo;
import group.bda.federate.sql.table.FederateTable;
import org.apache.calcite.linq4j.tree.Types;

public enum FedSpatialMethod {
  FED_SPATIAL_QUERY(FederateTable.FederateQueryable.class, "query", List.class, String.class,
      List.class, Integer.class, Integer.class, List.class),
  FED_SPATIAL_JOIN(FederateTable.FederateQueryable.class, "join", FedSpatialRel.SingleQuery.class, FedSpatialRel.SingleQuery.class, FedSpatialJoinInfo.class, List.class, List.class, Integer.class, Integer.class, List.class);

  public final Method method;

  public static final ImmutableMap<Method, FedSpatialMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, FedSpatialMethod> builder = ImmutableMap.builder();
    for (FedSpatialMethod value : FedSpatialMethod.values()) {
      builder.put(value.method, value);
    }
    MAP = builder.build();
  }

  FedSpatialMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }
}
