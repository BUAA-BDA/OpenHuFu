package group.bda.federate.sql.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.TextFormat;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;

import group.bda.federate.data.Header;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.sql.expression.FedSpatialExpression;
import group.bda.federate.sql.expression.FedSpatialExpressions;
import group.bda.federate.sql.join.FedSpatialJoinInfo;
import group.bda.federate.sql.table.FederateTable;

public interface FedSpatialRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("FEDSPATIAL", FedSpatialRel.class);

  class Implementor {
    FedSpatialExpressions selectExps;
    FedSpatialExpression filterExp;
    int offset = 0;
    int fetch = Integer.MAX_VALUE;
    final List<String> order = new ArrayList<>();
    FedSpatialJoinInfo joinInfo;

    RelOptTable table;
    FederateTable federateTable;
    Header header;

    List<TableQueryParams> queryParams = new ArrayList<>();

    public void visitChild(int ordinal, RelNode input) {
      ((FedSpatialRel) input).implement(this);
    }

    public void setTable(RelOptTable table) {
      this.table = table;
    }

    public void setFederateTable(FederateTable federateTable) {
      this.federateTable = federateTable;
      this.header = federateTable.getHeader();
    }

    public void setHeader(Header header) {
      this.header = header;
    }

    public void setSelectExps(FedSpatialExpressions selects) {
      this.selectExps = selects;
    }

    public void addAggregate(List<AggregateCall> aggCalls) {
      this.selectExps.appendAggregate(aggCalls);
    }


    public void setFilterExp(FedSpatialExpression filter) {
      this.filterExp = filter;
    }

    public void setJoinInfo(FedSpatialJoinInfo info) {
      this.joinInfo = info;
    }

    public void addOrder(List<String> newOrder) {
      order.addAll(newOrder);
    }

    public Header getHeader() {
      return header;
    }

    public FedSpatialExpressions getSelectExps() {
      return selectExps;
    }

    public FedSpatialExpression getFilterExp() {
      return filterExp;
    }

    public FedSpatialJoinInfo getJoinInfo() {
      return joinInfo;
    }

    public void packTableQueryParams() {
      queryParams.add(new TableQueryParams(selectExps, filterExp, table, federateTable));
      this.table = null;
      this.federateTable = null;
      this.selectExps = null;
      this.filterExp = null;
    }

    public TableQueryParams getQueryParams(int index) {
      return queryParams.get(index);
    }

    public SingleQuery getSingleQuery(int index) {
      return queryParams.get(index).toSingleQuery();
    }

    public boolean hasJoin() {
      return joinInfo != null;
    }
  }

  class TableQueryParams {
    FedSpatialExpressions projectExps;
    FedSpatialExpression filterExp;
    RelOptTable table;
    FederateTable federateTable;

    TableQueryParams(FedSpatialExpressions projects, FedSpatialExpression filter, RelOptTable table, FederateTable federateTable) {
      this.projectExps = projects;
      this.filterExp = filter;
      this.table = table;
      this.federateTable = federateTable;
    }

    public FedSpatialExpressions getProjectExps() {
      return projectExps;
    }

    public FedSpatialExpression getFilterExp() {
      return filterExp;
    }

    public int getColumnSize() {
      return projectExps.size();
    }

    public String getTableName() {
      return federateTable.getTableName();
    }

    public Header getHeader() {
      return federateTable.getHeader();
    }

    public RelOptTable getTable() {
      return table;
    }

    public FederateTable getFederateTable() {
      return federateTable;
    }

    public SingleQuery toSingleQuery() {
      return new SingleQuery(projectExps.getExpStrings(), filterExp == null ? null : filterExp.toString(), getTableName());
    }
  }

  public class SingleQuery {
    final public List<String> project;
    final public String filter;
    final public String tableName;

    public SingleQuery(List<String> project, String filter, String tableName) {
      this.project = project;
      this.filter = filter;
      this.tableName = tableName;
    }

    public List<Expression> getProjectExps() {
      return project.stream().map(this::parse).collect(Collectors.toList());
    }

    public Expression getFilterExp() {
      if (filter == null) {
        return null;
      }
      return parse(filter);
    }

    public Expression parse(String exp) {
      Expression.Builder builder = Expression.newBuilder();
      try {
        TextFormat.getParser().merge(exp, builder);
      } catch (TextFormat.ParseException e) {
        e.printStackTrace();
      }
      return builder.build();
    }
  }
}
