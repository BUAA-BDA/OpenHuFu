package com.hufudb.openhufu.core.table;

import com.hufudb.openhufu.core.sql.rel.FQTable;
import com.hufudb.openhufu.core.sql.schema.FQSchemaManager;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FQTableFactory implements TableFactory {
  public static final FQTableFactory INSTANCE = new FQTableFactory();
  private static final Logger LOG = LoggerFactory.getLogger(FQTableFactory.class);

  private FQTableFactory() {}

  @Override
  public Table create(SchemaPlus schema, String tableName, Map operand, RelDataType rowType) {
    LOG.debug("create table {}", tableName);
    final FQSchemaManager schemaManager = schema.unwrap(FQSchemaManager.class);
    if (operand.get("components") == null) {
      throw new RuntimeException("components is null");
    }
    final RelProtoDataType protoRowType = rowType != null ? RelDataTypeImpl.proto(rowType) : null;
    return FQTable.create(schemaManager, tableName, operand, protoRowType);
  }

  public Table create(FQSchemaManager schema, GlobalTableConfig meta) {
    LOG.debug("create table {}", meta.tableName);
    return FQTable.create(schema, meta);
  }
}
