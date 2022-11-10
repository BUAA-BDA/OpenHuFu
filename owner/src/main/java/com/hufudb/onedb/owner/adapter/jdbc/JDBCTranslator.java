package com.hufudb.onedb.owner.adapter.jdbc;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.expression.Translator;
import com.hufudb.onedb.proto.OneDBPlan.Collation;
import com.hufudb.onedb.proto.OneDBPlan.Expression;

public class JDBCTranslator {
  Translator translator;

  JDBCTranslator(Translator translator) {
    this.translator = translator;
  }

  public List<String> translateExps(Schema schema, List<Expression> exps) {
    translator.setInput(schema.getColumnDescs().stream().map(col -> col.getName()).collect(Collectors.toList()));
    return exps.stream().map(exp -> translator.translate(exp)).collect(Collectors.toList());
  }

  public List<String> translateAgg(List<String> selectExpStrs, List<Expression> aggs) {
    translator.setInput(selectExpStrs);
    return aggs.stream().map(exp -> translator.translate(exp)).collect(Collectors.toList());
  }

  public List<String> translateOrders(List<String> outExpStrs, List<Collation> orders) {
    return orders.stream().map(order -> String.format("%s %s", outExpStrs.get(order.getRef()),
        order.getDirection().toString())).collect(Collectors.toList());
  }
}
