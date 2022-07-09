package com.hufudb.onedb.owner.adapter.postgis;

import java.sql.Connection;
import java.sql.Statement;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.expression.PostgisTranslator;

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
    final List<String> filters = PostgistTranslator.translateExps(tableSchema, plan.getWhereExps());
    final List<String> selects = PostgistTranslator.translateExps(tableSchema, plan.getSelectExps());
    final List<String> groups =
        plan.getGroups().stream().map(ref -> selects.get(ref)).collect(Collectors.toList());
    // order by
    List<String> order = PostgistTranslator.translateOrders(selects, plan.getOrders());
    StringBuilder sql = new StringBuilder();
    // select from clause
    if (!plan.getAggExps().isEmpty()) {
      final List<String> aggs = PostgistTranslator.translateAgg(selects, plan.getAggExps());
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
}
