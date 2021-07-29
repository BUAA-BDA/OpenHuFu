package group.bda.federate.sql.enumerator;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import group.bda.federate.sql.schema.FederateSchema;
import group.bda.federate.sql.join.FedSpatialJoinInfo;
import group.bda.federate.sql.operator.FedSpatialRel;
import org.apache.calcite.linq4j.Enumerator;

import group.bda.federate.data.Row;
import group.bda.federate.client.FedSpatialClient;
import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.sql.expression.FedSpatialExpression;
import group.bda.federate.sql.functions.AggregateType;

public class FederateEnumerator implements Enumerator<Object> {
  private final Enumerator<Row> enumerator;
  private final int limitCount;
  private int cnt = 0;

  public FederateEnumerator(String tableName, FederateSchema schema, List<String> project, String filter, List<Map.Entry<AggregateType, List<Integer>>> aggregateFields, int limitCount, List<String> order) {
    this.limitCount = limitCount;
    List<Expression> projectExps = FedSpatialExpression.str2Proto(project);
    FedSpatialClient client = schema.getClient();
    if (filter.isEmpty()) {
      enumerator = client.fedSpatialQuery(tableName, projectExps, ImmutableList.of(), aggregateFields, this.limitCount, order);
    } else {
      enumerator = client.fedSpatialQuery(tableName, projectExps, ImmutableList.of(FedSpatialExpression.str2Proto(filter)), aggregateFields, this.limitCount, order);
    }
  }

  public FederateEnumerator(FederateSchema schema, FedSpatialRel.SingleQuery left, FedSpatialRel.SingleQuery right, FedSpatialJoinInfo join, List<Integer> projects, List<Map.Entry<AggregateType, List<Integer>>> aggregateFields, Integer fetch, List<String> order) {
    FedSpatialClient client = schema.getClient();
    this.limitCount = fetch;
    enumerator = client.fedSpatialJoin(left, right, join, projects, aggregateFields, limitCount, order);
  }

  public FederateEnumerator(String tableName, FederateSchema schema) {
    this(tableName, schema, ImmutableList.of(), "", ImmutableList.of(), Integer.MAX_VALUE, ImmutableList.of());
  }

  @Override
  public Object current() {
    Row current = enumerator.current();
    if (current.size() == 1) {
      return current.getObject(0);
    }
    Object[] row = current.copyValues();
    return row;
  }

  @Override
  public boolean moveNext() {
    cnt++;
    if (cnt > this.limitCount && this.limitCount >= 0) {
      return false;
    }
    return enumerator.moveNext();
  }

  @Override
  public void reset() {
    cnt = 0;
    enumerator.reset();
  }

  @Override
  public void close() {
    // doing nothing
  }
}
