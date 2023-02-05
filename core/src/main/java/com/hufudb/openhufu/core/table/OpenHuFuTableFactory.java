package com.hufudb.openhufu.core.table;

import com.hufudb.openhufu.core.sql.rel.OpenHuFuTable;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaManager;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenHuFuTableFactory implements TableFactory {
  public static final OpenHuFuTableFactory INSTANCE = new OpenHuFuTableFactory();
  private static final Logger LOG = LoggerFactory.getLogger(OpenHuFuTableFactory.class);

  private OpenHuFuTableFactory() {}

  @Override
  public Table create(SchemaPlus schema, String tableName, Map operand, RelDataType rowType) {
    LOG.debug("create table {}", tableName);
    final OpenHuFuSchemaManager schemaManager = schema.unwrap(OpenHuFuSchemaManager.class);
    if (operand.get("components") == null) {
      throw new RuntimeException("components is null");
    }
    final RelProtoDataType protoRowType = rowType != null ? RelDataTypeImpl.proto(rowType) : null;
    return OpenHuFuTable.create(schemaManager, tableName, operand, protoRowType);
  }

  public Table create(OpenHuFuSchemaManager schema, GlobalTableConfig meta) {
    LOG.debug("create table {}", meta.tableName);
    return OpenHuFuTable.create(schema, meta);
  }
}
