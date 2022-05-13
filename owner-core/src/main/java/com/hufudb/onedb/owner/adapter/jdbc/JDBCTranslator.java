package com.hufudb.onedb.owner.adapter.jdbc;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.expression.BasicTranslator;
import com.hufudb.onedb.expression.Translator;
import com.hufudb.onedb.proto.OneDBPlan.Collation;
import com.hufudb.onedb.proto.OneDBPlan.Expression;

public class JDBCTranslator {
  JDBCTranslator() {}

  public static List<String> translateExps(Schema schema, List<Expression> exps) {
    Translator trans = new BasicTranslator(schema);
    return exps.stream().map(exp -> trans.translate(exp)).collect(Collectors.toList());
  }

  public static List<String> translateAgg(List<String> selectExpStrs, List<Expression> aggs) {
    Translator trans = new BasicTranslator(selectExpStrs);
    return aggs.stream().map(exp -> trans.translate(exp)).collect(Collectors.toList());
  }

  public static List<String> translateOrders(List<String> outExpStrs, List<Collation> orders) {
    return orders.stream().map(order -> String.format("%s %s", outExpStrs.get(order.getRef()),
        order.getDirection().toString())).collect(Collectors.toList());
  }
}
