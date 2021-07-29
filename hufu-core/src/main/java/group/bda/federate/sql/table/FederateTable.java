package group.bda.federate.sql.table;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

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
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.client.FederateDBClient;
import group.bda.federate.data.Header;
import group.bda.federate.sql.enumerator.FederateEnumerator;
import group.bda.federate.sql.functions.AggregateType;
import group.bda.federate.sql.join.FedSpatialJoinInfo;
import group.bda.federate.sql.operator.FedSpatialRel;
import group.bda.federate.sql.operator.FederateTableScan;
import group.bda.federate.sql.schema.FederateSchema;
import group.bda.federate.sql.type.FederateTypeConverter;

public class FederateTable extends AbstractQueryableTable implements TranslatableTable {

  private static final Logger LOG = LogManager.getLogger(FederateTable.class);

  final String tableName;
  final FederateSchema schema;
  final RelProtoDataType protoRowType;
  final FederateTableInfo tableInfo;

  // final List

  FederateTable(String tableName, FederateSchema schema, Header header, RelProtoDataType protoRowType) {
    super(Object[].class);
    this.tableName = tableName;
    this.schema = schema;
    this.protoRowType = protoRowType;
    this.tableInfo = new FederateTableInfo(tableName, header);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public void addFederate(FederateDBClient client, String localName) {
    tableInfo.addFed(client, localName);
  }

  // todo: add federate after init

  static Table create(FederateSchema schema, String tableName, Header header, RelProtoDataType protoRowType) {
    return new FederateTable(tableName, schema, header, protoRowType);
  }

  static public RelProtoDataType getRelDataType(FederateDBClient client, String localName) {
    Header header = client.getTableHeader(localName);
    return getRelDataType(header);
  }

  static public RelProtoDataType getRelDataType(Header header) {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    for (int i = 0; i < header.size(); ++i) {
      final SqlTypeName typeName = FederateTypeConverter.convert2SqlType(header.getType(i));
      fieldInfo.add(header.getName(i), typeName).nullable(true);
    }
    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  static Table create(FederateSchema schema, TableMeta tableMeta) {
    final String tableName = tableMeta.tableName;
    FederateTable table = null;
    for (TableMeta.FedMeta fedMeta : tableMeta.feds) {
      FederateDBClient client = schema.getDBClient(fedMeta.endpoint);
      if (client == null) {
        throw new RuntimeException("endpoint not exist");
      }
      Header header = client.getTableHeader(fedMeta.localName);
      LOG.info("{}: header {} from [{} : {}]", tableName, header.toString(), fedMeta.endpoint, fedMeta.localName);
      if (table == null) {
        RelProtoDataType dataType = getRelDataType(client, fedMeta.localName);
        table = new FederateTable(tableName, schema, header, dataType);
        table.addFederate(client, fedMeta.localName);
        schema.addTable(tableName, table);
      } else {
        if (table.getHeader().equals(header)) {
          table.addFederate(client, fedMeta.localName);
        } else {
          LOG.warn("header in {} mismatch", fedMeta.endpoint);
        }
      }
    }
    if (table == null) {
      throw new RuntimeException("table init failed");
    }
    return table;
  }

  static Table create(FederateSchema schema, String tableName, Map operand, RelProtoDataType protoRowType) {
    List<LinkedHashMap<String, Object>> feds = (List<LinkedHashMap<String, Object>>) operand.get("feds");
    FederateTable table = null;
    for (LinkedHashMap<String, Object> fed : feds) {
      String endpoint = fed.get("endpoint").toString();
      String localName = fed.get("name").toString();
      FederateDBClient client = schema.getDBClient(endpoint);
      if (client == null) {
        throw new RuntimeException("endpoint not exist");
      }
      Header header = client.getTableHeader(localName);
      LOG.info("{}: header {} from [{} : {}]", tableName, header.toString(), endpoint, localName);
      if (table == null) {
        RelProtoDataType dataType = getRelDataType(client, localName);
        table = new FederateTable(tableName, schema, header, dataType);
        table.addFederate(client, localName);
        schema.addTable(tableName, table);
      } else {
        if (table.getHeader().equals(header)) {
          table.addFederate(client, localName);
        } else {
          LOG.warn("header in {} of {} mismatch", endpoint, tableName);
        }
      }
    }
    if (table == null) {
      throw new RuntimeException("table init failed");
    }
    return table;
  }

  public FederateTableInfo getTableInfo() {
    return tableInfo;
  }

  public Header getHeader() {
    return tableInfo.getHeader();
  }

  public String getTableName() {
    return tableInfo.getName();
  }

  @Override
  public String toString() {
    return String.format("FederateTable {%s}", tableName);
  }

  public Enumerable<Object> query() {
    System.out.println("I should not be here");
    return query(ImmutableList.of(), "", ImmutableList.of(), 0, Integer.MAX_VALUE, ImmutableList.of());
  }

  public Enumerable<Object> join(FedSpatialRel.SingleQuery left, FedSpatialRel.SingleQuery right, FedSpatialJoinInfo joinInfo,
                                 List<Integer> project, List<Map.Entry<AggregateType, List<Integer>>> aggregateFields, final Integer offset, final Integer fetch, List<String> order) {
    return new AbstractEnumerable<Object>() {
      @Override
      public Enumerator<Object> enumerator() {
        return new FederateEnumerator(schema, left, right, joinInfo, project, aggregateFields, fetch, order);
      }
    };
  }

  public Enumerable<Object> query(List<String> project, String filter, List<Map.Entry<AggregateType,
          List<Integer>>> aggregateFields, final Integer offset, final Integer fetch, List<String> order) {
    return new AbstractEnumerable<Object>() {
      Enumerator<Object> enumerator;
      @Override
      public Enumerator<Object> enumerator() {
        if (enumerator == null) {
          this.enumerator = new FederateEnumerator(tableName, schema, project, filter, aggregateFields, fetch, order);
        } else {
          this.enumerator.reset();
        }
        return this.enumerator;
      }
    };
  }

  public static class FederateQueryable<T> extends AbstractTableQueryable<T> {
    public FederateQueryable(QueryProvider queryProvider, SchemaPlus schema, FederateTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    private FederateTable getTable() {
      return (FederateTable) table;
    }

    @Override
    public Enumerator<T> enumerator() {
      final Enumerable<T> enumerable = (Enumerable<T>) getTable().query();
      return enumerable.enumerator();
    }

    public Enumerable<Object> query(List<String> project, String filter, List<Map.Entry<AggregateType,
            List<Integer>>> aggregateFields, Integer offset, Integer fetch, List<String> order) {
      return getTable().query(project, filter, aggregateFields, offset, fetch, order);
    }

    public Enumerable<Object> join(FedSpatialRel.SingleQuery left, FedSpatialRel.SingleQuery right, FedSpatialJoinInfo joinInfo,
                                          List<Integer> project, List<Map.Entry<AggregateType, List<Integer>>> aggregateFields, final Integer offset, final Integer fetch, List<String> order) {
      return getTable().join(left, right, joinInfo, project, aggregateFields, offset, fetch, order);
    }
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new FederateQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new FederateTableScan(cluster, cluster.traitSetOf(FedSpatialRel.CONVENTION), relOptTable, this, null);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.of(1000.0, ImmutableList.of());
  }
}
