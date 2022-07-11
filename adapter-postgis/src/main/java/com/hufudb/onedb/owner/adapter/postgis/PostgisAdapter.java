package com.hufudb.onedb.owner.adapter.postgis;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.EmptyDataSet;
import com.hufudb.onedb.data.storage.ResultDataSet;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.owner.adapter.Adapter;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.owner.adapter.jdbc.JDBCAdapter;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgisAdapter extends JDBCAdapter {
  PostgisAdapter(String catalog, Connection connection, Statement statement,
      AdapterTypeConverter converter) {
    super(catalog, connection, statement, converter);
  }

  @Override
  protected String generateSQL(Plan plan) {
    assert plan.getPlanType().equals(PlanType.LEAF);
    String actualTableName = schemaManager.getActualTableName(plan.getTableName());
    Schema tableSchema = schemaManager.getActualSchema(plan.getTableName());
    LOG.info("Query {}: {}", actualTableName, tableSchema);
    final List<String> filters = PostgisJDBCTranslator.translateExps(tableSchema, plan.getWhereExps());
    final List<String> selects = PostgisJDBCTranslator.translateExps(tableSchema, plan.getSelectExps());
    final List<String> groups =
        plan.getGroups().stream().map(ref -> selects.get(ref)).collect(Collectors.toList());
    // order by
    List<String> order = PostgisJDBCTranslator.translateOrders(selects, plan.getOrders());
    StringBuilder sql = new StringBuilder();
    // select from clause
    if (!plan.getAggExps().isEmpty()) {
      final List<String> aggs = PostgisJDBCTranslator.translateAgg(selects, plan.getAggExps());
      sql.append(String.format("SELECT %s from %s", String.join(",", aggs), actualTableName));
    } else {
      sql.append(String.format("SELECT %s from %s", String.join(",", selects), actualTableName));
    }

    /**
     * todo:
     * add support to scalar function
     */

    // where clause
    if (!filters.isEmpty()) {
      sql.append(String.format(" where %s", String.join(" AND ", filters)));
    }
    if (!groups.isEmpty()) {
      sql.append(String.format(" group by %s", String.join(",", groups)));
    }
    if (!order.isEmpty()) {
      sql.append(String.format(" order by %s", String.join(",", order)));
    }
    if (plan.getFetch() != 0) {
      sql.append(" LIMIT ").append(plan.getFetch() + plan.getOffset());
    }
    return sql.toString();
  }

  public String testGenerateSQL(Plan p) {
    return this.generateSQL(p);
  }
}
