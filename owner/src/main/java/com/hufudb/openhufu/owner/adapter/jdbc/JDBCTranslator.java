package com.hufudb.openhufu.owner.adapter.jdbc;

import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.expression.Translator;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Collation;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import java.util.List;
import java.util.stream.Collectors;

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
