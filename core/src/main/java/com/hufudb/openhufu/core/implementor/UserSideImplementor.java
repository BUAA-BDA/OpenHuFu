package com.hufudb.openhufu.core.implementor;

import com.hufudb.openhufu.core.client.OpenHuFuClient;
import com.hufudb.openhufu.core.client.OwnerClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import com.hufudb.openhufu.core.sql.plan.PlanUtils;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.LimitDataSet;
import com.hufudb.openhufu.data.storage.MultiSourceDataSet;
import com.hufudb.openhufu.data.storage.SortedDataSet;
import com.hufudb.openhufu.data.storage.MultiSourceDataSet.Producer;
import com.hufudb.openhufu.implementor.PlanImplementor;
import com.hufudb.openhufu.interpreter.Interpreter;
import com.hufudb.openhufu.plan.BinaryPlan;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.UnaryPlan;
import com.hufudb.openhufu.proto.OpenHuFuData.DataSetProto;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.PlanType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.QueryPlanProto;
import org.apache.commons.lang3.tuple.Pair;

public class UserSideImplementor implements PlanImplementor {

  protected final OpenHuFuClient client;

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

  @Override
  public DataSet implement(Plan plan) {
    if (isMultiParty(plan)) {
      // implement on owner side
      return ownerSideQuery(plan);
    } else {
      // implement on user side
      return plan.implement(this);
    }
  }

  @Override
  public DataSet binaryQuery(BinaryPlan binary) {
    List<Plan> children = binary.getChildren();
    assert children.size() == 2;
    Plan left = children.get(0);
    Plan right = children.get(1);
    DataSet leftResult = implement(left);
    DataSet rightResult = implement(right);
    // DataSet result = leftResult.join(this, rightResult, binary.getJoinCond());
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
    MultiSourceDataSet concurrentDataSet = new MultiSourceDataSet(leaf.getOutSchema(), plans.size());
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
