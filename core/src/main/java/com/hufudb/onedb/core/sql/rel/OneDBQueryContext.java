package com.hufudb.onedb.core.sql.rel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;

public class OneDBQueryContext {
  // 
  // global context management
  //
  // global OneDBQueryContext pool, queryContext are saved in this structure
  static final Map<Long, OneDBQueryContext> contexts = new ConcurrentHashMap<>();
  private static AtomicLong counter = new AtomicLong(0);

  public static OneDBQueryContext getContext(long cid) {
    return contexts.get(cid);
  }

  public static void saveContext(OneDBQueryContext context) {
    contexts.put(context.getContextId(), context);
  }

  public static void deleteContext(long id) {
    contexts.remove(id);
  }

  //
  // context for a single query
  //
  final long id;
  OneDBQueryProto.Builder builder;

  OneDBQueryContext(OneDBQueryProto.Builder builder) {
    counter.compareAndSet(Long.MAX_VALUE, 0);
    id = counter.addAndGet(1);
    this.builder = builder;
  }

  public OneDBQueryContext() {
    counter.compareAndSet(Long.MAX_VALUE, 0);
    id = counter.addAndGet(1);
    builder = OneDBQueryProto.newBuilder();
  }

  public static OneDBQueryContext fromProto(OneDBQueryProto.Builder proto) {
    return new OneDBQueryContext(proto);
  }

  public static OneDBQueryContext fromProto(OneDBQueryProto proto) {
    return new OneDBQueryContext(proto.toBuilder());
  }

  public static OneDBQueryContext fromProto(String proto) {
    OneDBQueryProto.Builder builder = OneDBQueryProto.newBuilder();
    try {
      TextFormat.getParser().merge(proto, builder);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return new OneDBQueryContext(builder);
  }

  public static Header generateHeader(OneDBQueryProto proto) {
    if (proto.getAggExpCount() > 0) {
      return OneDBExpression.generateHeader(proto.getAggExpList());
    } else {
      return OneDBExpression.generateHeader(proto.getSelectExpList());
    }
  }

  public OneDBQueryProto toProto() {
    return builder.build();
  }

  public String toProtoStr() {
    return builder.build().toString();
  }

  public long getContextId() {
    return id;
  }
}
