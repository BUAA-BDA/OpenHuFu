package com.hufudb.onedb.core.sql.rel;

import java.util.List;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBQuery;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

public interface OneDBRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("OneDB", OneDBRel.class);

  class Implementor {
    OneDBQuery currentQuery;

    Implementor() {
      currentQuery = new OneDBQuery();
    }

    public OneDBQuery getQuery() {
      return currentQuery;
    }

    public void visitChild(int ordinal, RelNode input) {
      ((OneDBRel) input).implement(this);
    }

    public void setTable(RelOptTable table) {
      this.currentQuery.table = table;
    }

    public void setOneDBTable(OneDBTable oneDBTable) {
      this.currentQuery.oneDBTable = oneDBTable;
      this.currentQuery.header = oneDBTable.getHeader();
    }

    public void setSelectExps(List<OneDBExpression> exps) {
      this.currentQuery.selectExps = exps;
    }

    public void addFilterExps(OneDBExpression exp) {
      this.currentQuery.filterExps.add(exp);
    }

    public void setAggExps(List<OneDBExpression> exps) {
      this.currentQuery.aggExps = exps;
    }

    public void setOffset(int offset) {
      this.currentQuery.offset = offset;
    }

    public void setFetch(int fetch) {
      this.currentQuery.fetch = fetch;
    }

    public Header getHeader() {
      return this.currentQuery.header;
    }

    public List<OneDBExpression> getCurrentOutput() {
      return this.currentQuery.selectExps;
    }
  }
}
