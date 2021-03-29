package tk.onedb.core.sql.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptTable;

import tk.onedb.core.data.Header;
import tk.onedb.core.sql.rel.OneDBTable;
import tk.onedb.rpc.OneDBCommon.OneDBQueryProto;

public class OneDBQuery {
  public Header header;
  public RelOptTable table;
  public OneDBTable oneDBTable;
  public List<OneDBExpression> selectExps;
  public List<OneDBExpression> filterExps;
  public List<OneDBExpression> aggExps;
  public int offset = 0;
  public Integer fetch = null;

  public OneDBQueryProto toProto() {
    OneDBQueryProto.Builder builder = OneDBQueryProto.newBuilder();
    builder.setTableName(oneDBTable.getTableName()).setHeader(header.toProto());
    builder.addAllSelectExp(selectExps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    builder.addAllWhereExp(filterExps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    builder.addAllAggExp(aggExps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    if (fetch != null) {
      builder.setFetch(fetch);
    }
    return builder.build();
  }

  public OneDBQuery() {
    this.selectExps = new ArrayList<>();
    this.filterExps = new ArrayList<>();
    this.aggExps = new ArrayList<>();
  }
}
