package group.bda.federate.sql.join;

import group.bda.federate.rpc.FederateCommon.Expression;
import group.bda.federate.sql.expression.FedSpatialExpression;
import group.bda.federate.sql.expression.FedSpatialExpressions;
import group.bda.federate.sql.type.Point;

public class FedSpatialDistanceJoinInfo extends FedSpatialJoinInfo {
  final public int leftKey;
  final public int rightKey;
  final public int leftSize;
  final public int rightSize;
  public String distanceFilter;
  public double distance;
  public boolean equal;

  public FedSpatialDistanceJoinInfo(int leftKey, int rightKey, FedSpatialExpressions leftExps, FedSpatialExpressions rightExps, double distance, boolean equal) {
    this.leftKey = leftKey;
    this.rightKey = rightKey;
    this.leftSize = leftExps.size();
    this.rightSize = rightExps.size();
    this.distance = distance;
    this.equal = equal;
    this.distanceFilter = generateFilter(rightExps);
  }

  public FedSpatialDistanceJoinInfo(int leftKey, int rightKey, int leftSize, int rightSize, String distanceFilter, double distance, boolean equal) {
    this.leftKey = leftKey;
    this.rightKey = rightKey;
    this.leftSize = leftSize;
    this.rightSize = rightSize;
    this.distanceFilter = distanceFilter;
    this.distance = distance;
    this.equal = equal;
  }

  private String generateFilter(FedSpatialExpressions rightExps) {
    Expression exp = FedSpatialExpression.createDistanceJoinFilterTemplate(rightKey, distance, equal, rightExps);
    return exp.toString();
  }

  public Expression getDistanceFilter(Point p) {
    return FedSpatialExpression.generateDistanceJoinFilter(distanceFilter, p);
  }

  @Override
  public JoinType getType() {
    return JoinType.DISTANCE;
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
