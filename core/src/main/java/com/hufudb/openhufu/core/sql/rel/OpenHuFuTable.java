package com.hufudb.openhufu.core.sql.rel;

import com.hufudb.openhufu.core.client.OwnerClient;
import com.hufudb.openhufu.core.data.TypeConverter;
import com.hufudb.openhufu.core.sql.enumerator.OpenHuFuEnumerator;
import com.hufudb.openhufu.core.sql.schema.OpenHuFuSchemaManager;
import com.hufudb.openhufu.core.table.GlobalTableConfig;
import com.hufudb.openhufu.core.table.LocalTableConfig;
import com.hufudb.openhufu.core.table.OpenHuFuTableSchema;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.schema.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenHuFuTable extends AbstractQueryableTable implements TranslatableTable {
  private static final Logger LOG = LoggerFactory.getLogger(OpenHuFuTable.class);

  final OpenHuFuSchemaManager schemaManager;
  final RelProtoDataType protoRowType;
  final OpenHuFuTableSchema tableInfo;

  OpenHuFuTable(String tableName, OpenHuFuSchemaManager schemaManager, Schema schema,
                RelProtoDataType protoRowType) {
    super(Object[].class);
    this.schemaManager = schemaManager;
    this.protoRowType = protoRowType;
    this.tableInfo = new OpenHuFuTableSchema(tableName, schema);
  }

  public static Table create(OpenHuFuSchemaManager schemaManager, GlobalTableConfig tableMeta) {
    final String tableName = tableMeta.tableName;
    if (schemaManager.hasTable(tableName)) {
      LOG.warn("Table {} already exists", tableName);
      return null;
    }
    OpenHuFuTable table = null;
    List<Pair<OwnerClient, TableSchema>> localInfos = new ArrayList<>();
    for (LocalTableConfig fedMeta : tableMeta.localTables) {
      OwnerClient client = schemaManager.getOwnerClient(fedMeta.endpoint);
      if (client == null) {
        LOG.error("No connection to owner {}", fedMeta.endpoint);
        continue;
      }
      Schema schema = client.getTableSchema(fedMeta.localName);
      if (schema.equals(Schema.EMPTY)) {
        LOG.warn("Table {} not exists in {}", fedMeta.localName, fedMeta.endpoint);
        continue;
      }
      LOG.info("Table {} schema {} from {}", fedMeta.localName, schema.toString(),
          fedMeta.endpoint);
      localInfos.add(Pair.of(client, TableSchema.of(fedMeta.localName, schema)));
    }
    if (localInfos.size() > 0) {
      TableSchema standard = localInfos.get(0).getValue();
      for (Pair<OwnerClient, TableSchema> pair : localInfos) {
        if (!standard.getSchema().equals(pair.getValue().getSchema())) {
          LOG.warn("Schema of {} {} {} mismatch with {} {} {}", localInfos.get(0).getKey(),
              localInfos.get(0).getValue().getName(), standard, pair.getKey(), standard.getName(),
              standard.getSchema());
          return null;
        }
      }
      RelProtoDataType dataType = getRelDataType(standard.getSchema());
      table = new OpenHuFuTable(tableName, schemaManager, standard.getSchema(), dataType);
      for (Pair<OwnerClient, TableSchema> pair : localInfos) {
        table.addOwner(pair.getKey(), pair.getValue().getName());
      }
      schemaManager.addTable(tableMeta.tableName, table);
    }
    if (table == null) {
      LOG.error("Fail to init table {}", tableMeta.tableName);
    } else {
      LOG.info("create global table {}", tableMeta.tableName);
    }
    return table;
  }

  public static Table create(OpenHuFuSchemaManager schemaManager, String tableName, Map operand,
                             RelProtoDataType protoRowType) {
    List<Map<String, Object>> components = (List<Map<String, Object>>) operand.get("components");
    OpenHuFuTable table = null;
    for (Map<String, Object> fed : components) {
      String endpoint = fed.get("endpoint").toString();
      String localName = fed.get("name").toString();
      OwnerClient client = schemaManager.getOwnerClient(endpoint);
      if (client == null) {
        LOG.warn("endpoint {} not exist", endpoint);
        throw new RuntimeException("endpoint not exist");
      }
      Schema schema = client.getTableSchema(localName);
      LOG.info("{}: schema {} from [{} : {}]", tableName, schema.toString(), endpoint, localName);
      if (table == null) {
        RelProtoDataType dataType = getRelDataType(schema);
        table = new OpenHuFuTable(tableName, schemaManager, schema, dataType);
        table.addOwner(client, localName);
        schemaManager.addTable(tableName, table);
      } else {
        if (table.getSchema().equals(schema)) {
          table.addOwner(client, localName);
        } else {
          LOG.warn("schema in {} mismatch with origin[{}]", endpoint, table.getSchema());
        }
      }
    }
    if (table == null) {
      LOG.error("Fail to init table {}", tableName);
    }
    return table;
  }

  public static RelProtoDataType getRelDataType(OwnerClient client, String localName) {
    Schema schema = client.getTableSchema(localName);
    return getRelDataType(schema);
  }

  public static RelProtoDataType getRelDataType(Schema schema) {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    for (int i = 0; i < schema.size(); ++i) {
      final SqlTypeName typeName = TypeConverter.convert2SqlType(schema.getType(i));
      fieldInfo.add(schema.getName(i), typeName).nullable(true);
    }
    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public void addOwner(OwnerClient client, String localName) {
    tableInfo.addLocalTable(client, localName);
  }

  public Enumerable<Object> query() {
    System.out.println("Should not use this method");
    return query(-1);
  }

  public Enumerable<Object> query(long planId) {
    return new AbstractEnumerable<Object>() {
      Enumerator<Object> enumerator;

      @Override
      public Enumerator<Object> enumerator() {
        if (enumerator == null) {
          this.enumerator = new OpenHuFuEnumerator(schemaManager, planId);
        } else {
          this.enumerator.reset();
        }
        return this.enumerator;
      }
    };
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new OpenHuFuQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new OpenHuFuTableScan(cluster, cluster.traitSetOf(OpenHuFuRel.CONVENTION), relOptTable, this,
        null);
  }

  public OpenHuFuTableSchema getTableSchema() {
    return tableInfo;
  }

  public Schema getSchema() {
    return tableInfo.getSchema();
  }

  public String getTableName() {
    return tableInfo.getName();
  }

  protected OpenHuFuSchemaManager getRootSchema() {
    return schemaManager;
  }

  public static class OpenHuFuQueryable<T> extends AbstractTableQueryable<T> {
    public OpenHuFuQueryable(QueryProvider queryProvider, SchemaPlus schema, OpenHuFuTable table,
                             String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    private OpenHuFuTable getTable() {
      return (OpenHuFuTable) table;
    }

    @Override
    public Enumerator<T> enumerator() {
      final Enumerable<T> enumerable = (Enumerable<T>) getTable().query();
      return enumerable.enumerator();
    }

    public Enumerable<Object> query(long contextId) {
      return getTable().query(contextId);
    }
  }
}
