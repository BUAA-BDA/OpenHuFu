package com.hufudb.onedb.core.sql.rel;

import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBReference;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
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

  public static Header getOutputHeader(OneDBQueryProto proto, OneDBSchema schema) {
    return OneDBExpression.generateHeader(getOutputExpressions(proto, schema));
  }

  public static List<OneDBExpression> getOutputExpressions(OneDBQueryProto proto, OneDBSchema schema) {
    if (hasJoin(proto)) {
      return getJoinOutput(proto);
    } else {
      return getSingleTableOutput(proto, schema.getHeader(proto.getTableName()));
    }
  }

  public static List<OneDBExpression> getJoinOutput(OneDBQueryProto proto) {
    if (proto.getAggExpCount() > 0) {
      if (proto.getGroupCount() > 0) {
        List<OneDBExpression> outputs = new ArrayList<>();
        outputs.addAll(proto.getGroupList().stream()
            .map(ref -> OneDBReference
                .fromIndex(FieldType.of(proto.getSelectExp(ref).getOutType()), ref))
            .collect(Collectors.toList()));
        outputs.addAll(OneDBExpression.fromProto(proto.getAggExpList()));
        return outputs;
      } else {
        return OneDBExpression.fromProto(proto.getAggExpList());
      }
    } else {
      return OneDBExpression.fromProto(proto.getSelectExpList());
    }
  }

  public static List<OneDBExpression> getSingleTableOutput(OneDBQueryProto proto, Header tableHeader) {
    if (proto.getAggExpCount() > 0) {
      if (proto.getGroupCount() > 0) {
        List<OneDBExpression> outputs = new ArrayList<>();
        outputs.addAll(proto.getGroupList().stream()
            .map(ref -> OneDBReference.fromIndex(tableHeader, ref))
            .collect(Collectors.toList()));
        outputs.addAll(OneDBExpression.fromProto(proto.getAggExpList()));
        return outputs;
      } else {
        return OneDBExpression.fromProto(proto.getAggExpList());
      }
    } else {
      return OneDBExpression.fromProto(proto.getSelectExpList());
    }
  }

  public static Header generateHeaderForSingleTable(OneDBQueryProto proto, Header tableHeader) {
    return OneDBExpression.generateHeader(getSingleTableOutput(proto, tableHeader));
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
