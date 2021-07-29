package group.bda.federate.sql.join;

import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.sql.expression.FedSpatialExpression;
import group.bda.federate.sql.expression.FedSpatialExpressions;
import group.bda.federate.sql.type.Point;

public class FedSpatialKnnJoinInfo extends FedSpatialJoinInfo {
  final public int leftKey;
  final public int rightKey;
  final public int leftSize;
  final public int rightSize;
  final public String kNNFilter;
  final public int k;

  public FedSpatialKnnJoinInfo(int leftKey, int rightKey, FedSpatialExpressions leftExps, FedSpatialExpressions rightExps, int k) {
    this.leftKey = leftKey;
    this.rightKey = rightKey;
    this.leftSize = leftExps.size();
    this.rightSize = rightExps.size();
    this.k = k;
    this.kNNFilter = generateKnnFilter(rightExps);
  }

  public FedSpatialKnnJoinInfo(int leftKey, int rightKey, int leftSize, int rightSize, String kNNFilter, int k) {
    this.leftKey = leftKey;
    this.rightKey = rightKey;
    this.leftSize = leftSize;
    this.rightSize = rightSize;
    this.kNNFilter = kNNFilter;
    this.k = k;
  }

  public Expression getKNNFilter(Point p) {
    return FedSpatialExpression.generateKNNJoinFilter(kNNFilter, p);
  }

  private String generateKnnFilter(FedSpatialExpressions rightExps) {
    Expression exp = FedSpatialExpression.createKNNJoinFilterTemplate(rightKey, k, rightExps);
    return exp.toString();
  }

  @Override
  public JoinType getType() {
    return JoinType.KNN;
  }

  @Override
  public int getLeftSize() {
    return leftSize;
  }

  @Override
  public int getRightSize() {
    return rightSize;
  }

  @Override
  public int getLeftKey() {
    return leftKey;
  }

  @Override
  public int getRightKey() {
    return rightKey;
  }
}
