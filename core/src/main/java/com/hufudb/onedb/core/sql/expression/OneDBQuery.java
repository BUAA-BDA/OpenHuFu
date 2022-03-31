package com.hufudb.onedb.core.sql.expression;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.rel.OneDBTable;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptTable;

// todo: remove this class and directly use OneDBQueryProto to reduce conversion overhead
@Deprecated
public class OneDBQuery {
  public String tableName;
  public Header header;
  public RelOptTable table;
  public OneDBTable oneDBTable;
  public List<OneDBExpression> selectExps;
  public List<OneDBExpression> filterExps;
  public List<OneDBExpression> aggExps;
  public int offset = 0;
  public int fetch = 0;
  public boolean hasJoin = false;
  public List<OneDBExpression> joinCondition;
  public OneDBQuery left;
  public OneDBQuery right;

  public OneDBQueryProto toProto() {
    OneDBQueryProto.Builder builder = OneDBQueryProto.newBuilder();
    builder.setTableName(oneDBTable.getTableName());
    builder.addAllSelectExp(
        selectExps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    builder.addAllWhereExp(
        filterExps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    builder.addAllAggExp(aggExps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    builder.setFetch(fetch);
    return builder.build();
  }

  public static OneDBQuery fromProto(OneDBQueryProto proto) {
    List<OneDBExpression> selectExps =
        proto.getSelectExpList().stream()
            .map(p -> OneDBExpression.fromProto(p))
            .collect(Collectors.toList());
    List<OneDBExpression> filterExps =
        proto.getWhereExpList().stream()
            .map(p -> OneDBExpression.fromProto(p))
            .collect(Collectors.toList());
    List<OneDBExpression> aggExps =
        proto.getAggExpList().stream()
            .map(p -> OneDBExpression.fromProto(p))
            .collect(Collectors.toList());
    int fetch = proto.getFetch();
    return new OneDBQuery(proto.getTableName(), selectExps, filterExps, aggExps, fetch);
  }

  public OneDBQuery() {
    this.selectExps = new ArrayList<>();
    this.filterExps = new ArrayList<>();
    this.aggExps = new ArrayList<>();
  }

  public OneDBQuery(
      String tableName,
      List<OneDBExpression> selectExps,
      List<OneDBExpression> filterExps,
      List<OneDBExpression> aggExps,
      int fetch) {
    this.tableName = tableName;
    this.selectExps = selectExps;
    this.filterExps = filterExps;
    this.aggExps = aggExps;
    this.fetch = fetch;
  }
}
