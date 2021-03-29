package tk.onedb.core.sql.schema;

import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

public class OneDBSchemaFactory implements SchemaFactory {
  public static final OneDBSchemaFactory INSTANCE = new OneDBSchemaFactory();

  private OneDBSchemaFactory() {
  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    List<String> endpoints = (List<String>) operand.get("endpoints");
    List<Map<String, Object>> tables = (List) operand.get("tables");
    return new OneDBSchema(endpoints, tables);
  }

  public OneDBSchema create(SchemaPlus parentSchema, List<String> endpoints) {
    return new OneDBSchema(endpoints);
  }
}
