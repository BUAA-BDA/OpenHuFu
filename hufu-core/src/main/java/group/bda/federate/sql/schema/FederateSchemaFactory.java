package group.bda.federate.sql.schema;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import group.bda.federate.sql.functions.SpatialFunctions;

public class FederateSchemaFactory implements SchemaFactory {
  public static final FederateSchemaFactory INSTANCE = new FederateSchemaFactory();

  private FederateSchemaFactory() {
  }

  private void addSpatialFunctions(SchemaPlus parentSchema) {
    for (Method spatialFunction : SpatialFunctions.class.getMethods()) {
      parentSchema.add(spatialFunction.getName(), ScalarFunctionImpl.create(SpatialFunctions.class, spatialFunction.getName()));
    }
  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    List<String> endpoints = (List<String>) operand.get("endpoints");
    addSpatialFunctions(parentSchema);
    List<Map<String, Object>> tables = (List) operand.get("tables");
    return new FederateSchema(endpoints, tables);
  }

  public FederateSchema create(SchemaPlus parentSchema, List<String> endpoints) {
    addSpatialFunctions(parentSchema);
    return new FederateSchema(endpoints);
  }
}
