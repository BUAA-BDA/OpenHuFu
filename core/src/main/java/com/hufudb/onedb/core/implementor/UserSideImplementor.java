package com.hufudb.onedb.core.implementor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Schema;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.StreamBuffer;
import com.hufudb.onedb.core.implementor.plaintext.PlaintextImplementor;
import com.hufudb.onedb.core.implementor.plaintext.PlaintextQueryableDataSet;
import com.hufudb.onedb.core.sql.context.OneDBBinaryContext;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBContextType;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;
import org.apache.commons.lang3.tuple.Pair;

public abstract class UserSideImplementor implements OneDBImplementor {

  protected final OneDBClient client;

  protected UserSideImplementor(OneDBClient client) {
    this.client = client;
  }

  public static OneDBImplementor getImplementor(OneDBContext context, OneDBClient client) {
    switch (context.getContextLevel()) {
      case PUBLIC:
      case PROTECTED:
        return new PlaintextImplementor(client);
      default:
        LOG.error("No implementor found for Level {}", context.getContextLevel().name());
        throw new UnsupportedOperationException(
            String.format("No implementor found for Level %s", context.getContextLevel().name()));
    }
  }

  boolean isMultiParty(OneDBContext context) {
    OneDBContextType type = context.getContextType();
    Level level = context.getContextLevel();
    switch (type) {
      case ROOT: // no operation in root context
        return false;
      case LEAF:
      case UNARY:
      case BINARY:
        // todo: refinement needed
        return !level.equals(Level.PUBLIC);
      default:
        LOG.error("Unsupport context type {}", type);
        throw new UnsupportedOperationException();
    }
  }

  QueryableDataSet ownerSideQuery(OneDBContext context) {
    // todo: send context to owner and get result as queryable dataset
    List<Pair<OwnerClient, QueryContextProto>> queries = context.generateOwnerContextProto(client);
    StreamBuffer<DataSetProto> iterator = new StreamBuffer<>(queries.size());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (Pair<OwnerClient, QueryContextProto> entry : queries) {
      tasks.add(() -> {
        try {
          Iterator<DataSetProto> it = entry.getLeft().query(entry.getRight());
          while (it.hasNext()) {
            iterator.add(it.next());
          }
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        } finally {
          iterator.finish();
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
    Schema header = OneDBContext.getOutputHeader(context);
    BasicDataSet localDataSet = BasicDataSet.of(header);
    while (iterator.hasNext()) {
      localDataSet.mergeDataSet(BasicDataSet.fromProto(iterator.next()));
    }
    return PlaintextQueryableDataSet.fromBasic(localDataSet);
  }

  @Override
  public QueryableDataSet implement(OneDBContext context) {
    if (isMultiParty(context)) {
      // implement on owner side
      return ownerSideQuery(context);
    } else {
      // implement on user side
      return context.implement(this);
    }
  }

  @Override
  public QueryableDataSet binaryQuery(OneDBBinaryContext binary) {
    List<OneDBContext> children = binary.getChildren();
    assert children.size() == 2;
    OneDBContext left = children.get(0);
    OneDBContext right = children.get(1);
    QueryableDataSet leftResult = implement(left);
    QueryableDataSet rightResult = implement(right);
    QueryableDataSet result = leftResult.join(this, rightResult, binary.getJoinInfo());
    if (!binary.getWhereExps().isEmpty()) {
      result = result.filter(this, binary.getWhereExps());
    }
    if (!binary.getSelectExps().isEmpty()) {
      result = result.project(this, binary.getSelectExps());
    }
    if (!binary.getAggExps().isEmpty()) {
      List<ColumnType> types = new ArrayList<>();
      types.addAll(left.getOutTypes());
      types.addAll(right.getOutTypes());
      result = result.aggregate(this, binary.getGroups(), binary.getAggExps(), types);
    }
    if (!binary.getOrders().isEmpty()) {
      result = result.sort(this, binary.getOrders());
    }
    if (binary.getFetch() > 0 || binary.getOffset() > 0) {
      result = result.limit(binary.getOffset(), binary.getFetch());
    }
    return result;
  }

  @Override
  public QueryableDataSet unaryQuery(OneDBUnaryContext unary) {
    List<OneDBContext> children = unary.getChildren();
    assert children.size() == 1;
    QueryableDataSet input = implement(children.get(0));
    if (!unary.getSelectExps().isEmpty()) {
      input = input.project(this, unary.getSelectExps());
    }
    if (!unary.getAggExps().isEmpty()) {
      input = input.aggregate(this, unary.getGroups(), unary.getAggExps(), children.get(0).getOutTypes());
    }
    if (!unary.getOrders().isEmpty()) {
      input = input.sort(this, unary.getOrders());
    }
    if (unary.getFetch() > 0 || unary.getOffset() > 0) {
      input = input.limit(unary.getOffset(), unary.getFetch());
    }
    return input;
  }

  @Override
  public QueryableDataSet leafQuery(OneDBLeafContext leaf) {
    QueryContextProto proto = leaf.toProto();
    List<Pair<OwnerClient, String>> tableClients = client.getTableClients(leaf.getTableName());
    StreamBuffer<DataSetProto> streamProto = tableQuery(proto, tableClients);

    Schema header = OneDBContext.getOutputHeader(leaf);
    // todo: optimze for streamDataSet
    BasicDataSet localDataSet = BasicDataSet.of(header);
    while (streamProto.hasNext()) {
      localDataSet.mergeDataSet(BasicDataSet.fromProto(streamProto.next()));
    }
    return PlaintextQueryableDataSet.fromBasic(localDataSet);
  }

  private StreamBuffer<DataSetProto> tableQuery(QueryContextProto query,
      List<Pair<OwnerClient, String>> tableClients) {
    StreamBuffer<DataSetProto> iterator = new StreamBuffer<>(tableClients.size());
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (Pair<OwnerClient, String> entry : tableClients) {
      tasks.add(() -> {
        try {
          QueryContextProto localQuery = query.toBuilder().setTableName(entry.getValue()).build();
          Iterator<DataSetProto> it = entry.getKey().query(localQuery);
          while (it.hasNext()) {
            iterator.add(it.next());
          }
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        } finally {
          iterator.finish();
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
    return iterator;
  }
}
