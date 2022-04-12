package com.hufudb.onedb.core.sql.context;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OneDBQueryContextPool {
  static final Map<Long, OneDBContext> contexts = new ConcurrentHashMap<>();

  public static OneDBContext getContext(long cid) {
    return contexts.get(cid);
  }

  public static void saveContext(OneDBRootContext context) {
    contexts.put(context.getContextId(), context);
  }

  public static void deleteContext(long id) {
    contexts.remove(id);
  }
}
