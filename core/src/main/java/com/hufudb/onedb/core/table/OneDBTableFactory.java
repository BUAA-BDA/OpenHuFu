package com.hufudb.onedb.core.table;

import com.hufudb.onedb.core.sql.rel.OneDBTable;
import com.hufudb.onedb.core.sql.schema.OneDBSchemaManager;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneDBTableFactory implements TableFactory {
  public static final OneDBTableFactory INSTANCE = new OneDBTableFactory();
  private static final Logger LOG = LoggerFactory.getLogger(OneDBTableFactory.class);

  private OneDBTableFactory() {}

  @Override
  public Table create(SchemaPlus schema, String tableName, Map operand, RelDataType rowType) {
    LOG.debug("create table {}", tableName);
    final OneDBSchemaManager schemaManager = schema.unwrap(OneDBSchemaManager.class);
    if (operand.get("components") == null) {
      throw new RuntimeException("components is null");
    }
    final RelProtoDataType protoRowType = rowType != null ? RelDataTypeImpl.proto(rowType) : null;
    return OneDBTable.create(schemaManager, tableName, operand, protoRowType);
  }

  public Table create(OneDBSchemaManager schema, GlobalTableConfig meta) {
    LOG.debug("create table {}", meta.tableName);
    return OneDBTable.create(schema, meta);
  }
}
