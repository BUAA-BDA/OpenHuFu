package com.hufudb.onedb.core.implementor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.data.StreamBuffer;
import com.hufudb.onedb.core.implementor.plaintext.PlaintextQueryableDataSet;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBContextType;
import com.hufudb.onedb.rpc.OneDBCommon.DataSetProto;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;
import org.apache.commons.lang3.tuple.Pair;

public abstract class UserSideImplementor implements OneDBImplementor {

  protected final OneDBClient client;

  protected UserSideImplementor(OneDBClient client) {
    this.client = client;
  }

  boolean isMultiParty(OneDBContext context) {
    OneDBContextType type = context.getContextType();
    Level level = context.getContextLevel();
    switch(type) {
      case ROOT: // no operation in root context
      case LEAF: // leaf context can be executed on single owner
        return false;
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
    Header header = OneDBContext.getOutputHeader(context);
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
}
