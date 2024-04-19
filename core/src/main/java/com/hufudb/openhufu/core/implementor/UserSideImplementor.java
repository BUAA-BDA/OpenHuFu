package com.hufudb.openhufu.core.implementor;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import com.hufudb.openhufu.core.client.OpenHuFuClient;
import com.hufudb.openhufu.core.client.OwnerClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.hufudb.openhufu.core.implementor.spatial.join.DistanceJoin;
import com.hufudb.openhufu.core.implementor.spatial.join.KNNJoin;
import com.hufudb.openhufu.core.implementor.spatial.knn.BinarySearchKNN;
import com.hufudb.openhufu.core.implementor.spatial.knn.KNNConverter;
import com.hufudb.openhufu.core.sql.plan.PlanUtils;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.*;
import com.hufudb.openhufu.data.storage.MultiSourceDataSet.Producer;
import com.hufudb.openhufu.expression.ExpressionUtils;
import com.hufudb.openhufu.implementor.PlanImplementor;
import com.hufudb.openhufu.interpreter.Interpreter;
import com.hufudb.openhufu.plan.BinaryPlan;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.UnaryPlan;
import com.hufudb.openhufu.proto.OpenHuFuData.DataSetProto;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.proto.OpenHuFuPlan.PlanType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.QueryPlanProto;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserSideImplementor implements PlanImplementor {

  private static final Logger LOG = LoggerFactory.getLogger(UserSideImplementor.class);

  private final OpenHuFuClient client;

  protected UserSideImplementor(OpenHuFuClient client) {
    this.client = client;
  }

  public static PlanImplementor getImplementor(Plan plan, OpenHuFuClient client) {
    switch (plan.getPlanModifier()) {
      case PUBLIC:
      case PROTECTED:
        return new UserSideImplementor(client);
      default:
        LOG.error("No implementor found for Modifier {}", plan.getPlanModifier().name());
        throw new UnsupportedOperationException(
            String.format("No implementor found for Modifier %s", plan.getPlanModifier().name()));
    }
  }

  boolean isMultiParty(Plan plan) {
    PlanType type = plan.getPlanType();
    Modifier modifier = plan.getPlanModifier();
    switch (type) {
      case ROOT: // no operation in root plan
        return false;
      case LEAF:
      case UNARY:
      case BINARY:
        // todo: refinement needed
        return !modifier.equals(Modifier.PUBLIC);
      default:
        LOG.error("Unsupport plan type {}", type);
        throw new UnsupportedOperationException();
    }
  }

  DataSet ownerSideQuery(Plan plan) {
    List<Pair<OwnerClient, QueryPlanProto>> queries = PlanUtils.generateOwnerPlans(client, plan);
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    Schema schema = plan.getOutSchema();
    MultiSourceDataSet concurrentDataSet = new MultiSourceDataSet(schema, queries.size());
    for (Pair<OwnerClient, QueryPlanProto> entry : queries) {
      tasks.add(() -> {
        final Producer producer = concurrentDataSet.newProducer();
        try {
          Iterator<DataSetProto> it = entry.getLeft().query(entry.getRight());
          while (it.hasNext()) {
            producer.add(it.next());
          }
          return true;
        } catch (Exception e) {
          LOG.error("Error occur while fetching data from owner", e);
          return false;
        } finally {
          producer.finish();
        }
      });
    }
    try {
      List<Future<Boolean>> statusList = client.getThreadPool().invokeAll(tasks);
      for (Future<Boolean> status : statusList) {
        if (!status.get()) {
          LOG.error("Error in owner side query");
        }
      }
    } catch (Exception e) { // NOSONAR
      LOG.error("Error in owner side query", e);
    }
    return concurrentDataSet;
  }

  private boolean isPrivacyRangeJoin(BinaryPlan plan) {
    if (plan.getJoinCond().getModifier().equals(Modifier.PUBLIC)) {
      return false;
    }
    if (!plan.getJoinCond().getCondition().getInList().isEmpty()
            && !plan.getJoinCond().getCondition().getIn(0).getModifier().equals(Modifier.PUBLIC)) {
      throw new OpenHuFuException(ErrorCode.RANGE_JOIN_LEFT_TABLE_NOT_PUBLIC);
    }
    return plan.getJoinCond().getCondition().getStr().equals("dwithin");
  }

  private boolean isPrivacyKNNJoin(BinaryPlan plan) {
    if (plan.getJoinCond().getModifier().equals(Modifier.PUBLIC)) {
      return false;
    }
    if (!plan.getJoinCond().getCondition().getInList().isEmpty()
            && !plan.getJoinCond().getCondition().getIn(0).getModifier().equals(Modifier.PUBLIC)) {
      throw new OpenHuFuException(ErrorCode.RANGE_JOIN_LEFT_TABLE_NOT_PUBLIC);
    }
    return plan.getJoinCond().getCondition().getStr().equals("knn");
  }

  private DataSet privacySpatialJoin(BinaryPlan plan, boolean isDistanceJoin) {
    return privacySpatialJoin(plan, isDistanceJoin, false);
  }

  private DataSet privacySpatialJoin(BinaryPlan plan, boolean isDistanceJoin,
      boolean isUsingKNNFunc) {
    DataSet left = ownerSideQuery(plan.getChildren().get(0));
    DataSetIterator leftIter = left.getIterator();
    List<ArrayRow> arrayRows = new ArrayList<>();

    boolean containsLeftKey = false;
    int leftKey = -1;
    for (OpenHuFuPlan.Expression expression : plan.getSelectExps()) {
      if (expression.getOpType().equals(OpenHuFuPlan.OperatorType.REF)
          && expression.getI32() == plan.getJoinCond().getCondition().getIn(0).getI32()) {
        containsLeftKey = true;
      }
    }
    if (!containsLeftKey) {
      for (int i = 0; i < plan.getChildren().get(0).getSelectExps().size(); i++) {
        if (plan.getChildren().get(0).getSelectExps().get(i).getI32() == plan.getJoinCond()
            .getCondition().getIn(0).getI32()) {
          leftKey = i;
          break;
        }
      }
    }

    boolean containsRightKey = false;
    int rightKey = -1;
    for (OpenHuFuPlan.Expression expression : plan.getSelectExps()) {
      if (expression.getOpType().equals(OpenHuFuPlan.OperatorType.REF)
          && expression.getI32() == plan.getJoinCond().getCondition().getIn(1).getI32()) {
        containsRightKey = true;
      }
    }
    if (!containsRightKey) {
      for (int i = 0; i < plan.getChildren().get(1).getSelectExps().size(); i++) {
        if (plan.getChildren().get(1).getSelectExps().get(i).getI32()
            == plan.getJoinCond().getCondition().getIn(1).getI32() - plan.getChildren().get(0)
            .getSelectExps().size()) {
          rightKey = i;
          break;
        }
      }
    }
    while (leftIter.next()) {
      int leftRef = plan.getJoinCond().getCondition().getIn(0).getI32();
      DataSet rightDataSet;
      if (isDistanceJoin) {
        rightDataSet = ownerSideQuery(
            DistanceJoin.generateDistanceQueryPlan(plan, leftIter.get(leftRef).toString(),
                rightKey));
      } else {
        rightDataSet = privacyKNN(
            (UnaryPlan) KNNJoin.generateKNNQueryPlan(plan, leftIter.get(leftRef).toString(),
                rightKey), isUsingKNNFunc);
      }
      DataSetIterator rightIter = rightDataSet.getIterator();
      while (rightIter.next()) {
        arrayRows.add(ArrayRow.merge(leftIter, rightIter, leftKey));
        LOG.info(ArrayRow.merge(leftIter, rightIter, leftKey).toString());
      }
    }
    Schema schema;
    schema = ExpressionUtils.createSchema(plan.getSelectExps());
    LOG.info(schema.toString());
    return new ArrayDataSet(schema, arrayRows);
  }

  @Override
  public DataSet implement(Plan plan) {
    LOG.info(plan.toString());
    boolean isUsingKNNFuc =
        plan instanceof LeafPlan && !plan.getWhereExps().isEmpty() && plan.getWhereExps().get(0)
            .getOpType().equals(OpenHuFuPlan.OperatorType.SCALAR_FUNC) && plan.getWhereExps().get(0)
            .getStr().equals("knn");
    if (isUsingKNNFuc) {
      plan = KNNConverter.convertKNN((LeafPlan) plan);
    }
    if (isMultiParty(plan)) {
      if (plan instanceof BinaryPlan && isPrivacyRangeJoin((BinaryPlan) plan)) {
        return privacySpatialJoin((BinaryPlan) plan, true);
      }
      if (plan instanceof BinaryPlan && isPrivacyKNNJoin((BinaryPlan) plan)) {
        return privacySpatialJoin((BinaryPlan) plan, false, isUsingKNNFuc);
      }
      if (plan instanceof UnaryPlan && isMultiPartySecureKNN((UnaryPlan) plan)) {
        return privacyKNN((UnaryPlan) plan, isUsingKNNFuc);
      }
      // implement on owner side
      DataSet dataset = ownerSideQuery(plan);
      return dataset;
    } else {
      // implement on user side
      DataSet dataset = plan.implement(this);
      return dataset;
    }
  }

  private DataSet privacyKNN(UnaryPlan plan, boolean isUsingKNNFunc) {
    LOG.info("Using binary-search KNN.");
    boolean USE_DP = false;
    int k = plan.getFetch();
    double left = 0;
    double right = 1000000;
//    if (USE_DP) {
    right = kNNRadiusQuery(plan) * 2;
//    }
    double deviation = 1e-10;
    int loop = 0;
    long count = 0L;
    if (USE_DP) {
      while (left + deviation <= right) {
        double mid = (left + right) / 2;
        LOG.debug("k: {} left: {} right: {} mid: {}", k, left, right, mid);
        Pair<Double, Double> res = dPRangeCount(plan);
        count = Math.round(res.getKey());
        if (Math.abs(res.getKey() - k) < res.getValue()) {
          LOG.debug("change method on loop {}", loop);
          break;
        }
        if (count > k) {
          right = mid;
        } else if (count < k) {
          left = mid;
        }
        loop++;
        LOG.debug("loop {} with result size {}", loop, count);
      }
    }
    while (left + deviation <= right) {
      double mid = (left + right) / 2;
      int sign = (int) privacyCompare(plan, mid, k);
      LOG.debug("loop {} with  sign {}", loop, sign);
      if (sign < 0) {
        left = mid;
      } else if (sign > 0) {
        right = mid;
      } else {
        LOG.info("kNN radius is {}", mid);
        DataSet dataSet = ArrayDataSet.materialize(kNNCircleRangeQuery(plan, mid, isUsingKNNFunc));
        return dataSet;
      }
      loop++;
    }
    LOG.info("kNN radius is {}", right);
    return kNNCircleRangeQuery(plan, right, isUsingKNNFunc);
  }

  private double kNNRadiusQuery(UnaryPlan plan) {
    //todo -sjz
    DataSetIterator dataSet =
        ownerSideQuery(BinarySearchKNN.generateKNNRadiusQueryPlan(plan)).getIterator();
    double right = 1000000;
    while (dataSet.next()) {
      double res = (double) dataSet.get(0);
      LOG.info(String.valueOf(res));
      if (right > res) {
        right = res;
      }
    }
    return right;
  }

  private Pair<Double, Double> dPRangeCount(UnaryPlan plan) {
    //todo -sjz
    ownerSideQuery(BinarySearchKNN.generateDPRangeCountPlan(plan));
    return null;
  }

  private long privacyCompare(UnaryPlan plan, double range, long k) {
    //todo -sjz now it is using secretSharingSum
    DataSetIterator dataSet =
        ownerSideQuery(BinarySearchKNN.generatePrivacyComparePlan(plan, range)).getIterator();
    dataSet.next();
    long res = (long) dataSet.get(0);
    return res - k;
  }

  private DataSet kNNCircleRangeQuery(UnaryPlan plan, double range, boolean isUsingKNNFunc) {
    return ownerSideQuery(
        BinarySearchKNN.generateKNNCircleRangeQueryPlan(plan, range, isUsingKNNFunc));
  }

  private boolean isMultiPartySecureKNN(UnaryPlan unary) {
    LeafPlan leaf = (LeafPlan) unary.getChildren().get(0);
    boolean hasLimit = leaf.getOffset() != 0 || leaf.getFetch() != 0;
    if (!hasLimit) {
      return false;
    }
    if (leaf.getOrders() == null || leaf.getOrders().size() < 1) {
      return false;
    }
    int orderRef = leaf.getOrders().get(0).getRef();
    if (!(
        leaf.getSelectExps().get(orderRef).getOpType().equals(OpenHuFuPlan.OperatorType.SCALAR_FUNC)
            && leaf.getSelectExps().get(orderRef).getStr().equals("distance"))) {
      return false;
    }
    if (leaf.getOrders().get(0).getDirection().equals(OpenHuFuPlan.Direction.ASC)) {
      LOG.info("This is a KNN query.");
      return true;
    }
    return false;
  }

  @Override
  public DataSet binaryQuery(BinaryPlan binary) {
    List<Plan> children = binary.getChildren();
    assert children.size() == 2;
    Plan left = children.get(0);
    Plan right = children.get(1);
    DataSet leftResult = implement(left);
    DataSet rightResult = implement(right);
    DataSet result = Interpreter.join(leftResult, rightResult, binary.getJoinCond());
    if (!binary.getWhereExps().isEmpty()) {
      result = Interpreter.filter(result, binary.getWhereExps());
    }
    if (!binary.getSelectExps().isEmpty()) {
      result = Interpreter.map(result, binary.getSelectExps());
    }
    if (!binary.getAggExps().isEmpty()) {
      result = Interpreter.aggregate(result, binary.getGroups(), binary.getAggExps());
    }
    if (!binary.getOrders().isEmpty()) {
      result = SortedDataSet.sort(result, binary.getOrders());
    }
    if (binary.getFetch() > 0 || binary.getOffset() > 0) {
      result = LimitDataSet.limit(result, binary.getOffset(), binary.getFetch());
    }
    return result;
  }

  @Override
  public DataSet unaryQuery(UnaryPlan unary) {
    List<Plan> children = unary.getChildren();
    assert children.size() == 1;
    DataSet input = implement(children.get(0));
    if (!unary.getSelectExps().isEmpty()) {
      input = Interpreter.map(input, unary.getSelectExps());
    }
    if (!unary.getAggExps().isEmpty()) {
      input = Interpreter.aggregate(input, unary.getGroups(), unary.getAggExps());
    }
    if (!unary.getOrders().isEmpty()) {
      input = SortedDataSet.sort(input, unary.getOrders());
    }
    if (unary.getFetch() > 0 || unary.getOffset() > 0) {
      input = LimitDataSet.limit(input, unary.getOffset(), unary.getFetch());
    }
    return input;
  }

  @Override
  public DataSet leafQuery(LeafPlan leaf) {
    List<Pair<OwnerClient, QueryPlanProto>> plans = PlanUtils.generateLeafOwnerPlans(client, leaf);
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    MultiSourceDataSet concurrentDataSet =
        new MultiSourceDataSet(leaf.getOutSchema(), plans.size());
    for (Pair<OwnerClient, QueryPlanProto> entry : plans) {
      tasks.add(() -> {
        final Producer producer = concurrentDataSet.newProducer();
        try {
          Iterator<DataSetProto> it = entry.getKey().query(entry.getValue());
          while (it.hasNext()) {
            LOG.debug("get dataset from owner {}", entry.getKey().getEndpoint());
            producer.add(it.next());
          }
          return true;
        } catch (Exception e) {
          LOG.error("Error occur while fetching data from owner", e);
          return false;
        } finally {
          producer.finish();
        }
      });
    }
    try {
      List<Future<Boolean>> statusList = client.getThreadPool().invokeAll(tasks);
      for (Future<Boolean> status : statusList) {
        if (!status.get()) {
          LOG.error("error in leafQuery");
        }
      }
    } catch (Exception e) { //NOSONAR
      LOG.error("Error in owner side query", e);
    }
    return concurrentDataSet;
  }
}
