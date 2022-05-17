package com.hufudb.onedb.core.implementor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.LimitDataSet;
import com.hufudb.onedb.data.storage.MultiSourceDataSet;
import com.hufudb.onedb.data.storage.SortedDataSet;
import com.hufudb.onedb.data.storage.MultiSourceDataSet.Producer;
import com.hufudb.onedb.implementor.PlanImplementor;
import com.hufudb.onedb.interpreter.Interpreter;
import com.hufudb.onedb.plan.BinaryPlan;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.plan.UnaryPlan;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;
import com.hufudb.onedb.proto.OneDBPlan.QueryPlanProto;
import org.apache.commons.lang3.tuple.Pair;

public class UserSideImplementor implements PlanImplementor {

  protected final OneDBClient client;

  protected UserSideImplementor(OneDBClient client) {
    this.client = client;
  }

  public static PlanImplementor getImplementor(Plan plan, OneDBClient client) {
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
    List<Pair<OwnerClient, QueryPlanProto>> queries = plan.generateOwnerContextProto(client);
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    Schema schema = plan.getOutSchema();
    MultiSourceDataSet concurrentDataSet = new MultiSourceDataSet(schema);
    // todo: wait for producer
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
          e.printStackTrace();
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
    } catch (Exception e) {
      LOG.error("Error in owner side query: {}", e.getMessage());
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
    QueryPlanProto query = leaf.toProto();
    List<Pair<OwnerClient, String>> tableClients = client.getTableClients(leaf.getTableName());
    MultiSourceDataSet concurrentDataSet = new MultiSourceDataSet(leaf.getOutSchema());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (Pair<OwnerClient, String> entry : tableClients) {
      tasks.add(() -> {
        final Producer producer = concurrentDataSet.newProducer();
        try {
          QueryPlanProto localQuery = query.toBuilder().setTableName(entry.getValue()).build();
          Iterator<DataSetProto> it = entry.getKey().query(localQuery);
          while (it.hasNext()) {
            producer.add(it.next());
          }
          return true;
        } catch (Exception e) {
          e.printStackTrace();
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
    } catch (Exception e) {
      LOG.error("Error in leafQuery for {}", e.getMessage());
    }
    return concurrentDataSet;
  }
}
