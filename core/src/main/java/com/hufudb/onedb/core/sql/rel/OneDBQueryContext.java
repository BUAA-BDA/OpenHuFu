package com.hufudb.onedb.core.sql.rel;

import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBReference;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class OneDBQueryContext {
  //
  // global context management
  //
  // global OneDBQueryContext pool, queryContext are saved in this structure
  static final Map<Long, OneDBQueryContext> contexts = new ConcurrentHashMap<>();
  private static final AtomicLong counter = new AtomicLong(0);
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

  public static OneDBQueryContext getContext(long cid) {
    return contexts.get(cid);
  }

  public static void saveContext(OneDBQueryContext context) {
    contexts.put(context.getContextId(), context);
  }

  public static void deleteContext(long id) {
    contexts.remove(id);
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

  public static boolean hasJoin(OneDBQueryProto proto) {
    return proto.hasLeft() && proto.hasRight();
  }

  public static Header getOutputHeader(OneDBQueryProto proto) {
    Header.Builder builder = Header.newBuilder();
    List<FieldType> types = getOutputTypes(proto);
    types.stream().forEach(type -> builder.add("", type));
    return builder.build();
  }

  public static List<FieldType> getOutputTypes(OneDBQueryProto proto) {
    if (proto.getAggExpCount() > 0) {
      return proto.getAggExpList().stream().map(agg -> FieldType.of(agg.getOutType())).collect(Collectors.toList());
    } else {
      return proto.getSelectExpList().stream().map(sel -> FieldType.of(sel.getOutType())).collect(Collectors.toList());
    }
  }

  public static List<FieldType> getOutputTypes(OneDBQueryProto proto, List<Integer> indexs) {
    if (proto.getAggExpCount() > 0) {
      return indexs.stream().map(id -> FieldType.of(proto.getAggExp(id).getOutType())).collect(Collectors.toList());
    } else {
      return indexs.stream().map(id -> FieldType.of(proto.getSelectExp(id).getOutType())).collect(Collectors.toList());
    }
  }

  public static List<OneDBExpression> getOutputExpressions(OneDBQueryProto proto) {
    if (proto.getAggExpCount() > 0) {
      return OneDBExpression.fromProto(proto.getAggExpList());
    } else {
      return OneDBExpression.fromProto(proto.getSelectExpList());
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
