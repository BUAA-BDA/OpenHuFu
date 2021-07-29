package group.bda.federate.sql.table;

import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.sql.schema.FederateSchema;

public class FederateTableFactory implements TableFactory {
  private static final Logger LOG = LogManager.getLogger(FederateTableFactory.class);
  public static final FederateTableFactory INSTANCE = new FederateTableFactory();

  private FederateTableFactory() {
  }

  @Override
  public Table create(SchemaPlus schema, String tableName, Map operand, RelDataType rowType) {
    LOG.debug("create table {}", tableName);
    final FederateSchema federateSchema = schema.unwrap(FederateSchema.class);
    if (operand.get("feds") == null) {
      throw new RuntimeException("feds is null");
    }
    final RelProtoDataType protoRowType = rowType != null ? RelDataTypeImpl.proto(rowType) : null;
    return FederateTable.create(federateSchema, tableName, operand, protoRowType);
  }

  public Table create(FederateSchema schema, TableMeta meta) {
    LOG.debug("create table {}", meta.tableName);
    return FederateTable.create(schema, meta);
  }
}
