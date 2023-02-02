package com.hufudb.openhufu.plan;

import java.util.List;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.implementor.PlanImplementor;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Collation;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinCondition;
import com.hufudb.openhufu.proto.OpenHuFuPlan.PlanType;
import com.hufudb.openhufu.proto.OpenHuFuPlan.QueryPlanProto;
import com.hufudb.openhufu.proto.OpenHuFuPlan.TaskInfo;
import com.hufudb.openhufu.rewriter.Rewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Plan {
  static final Logger LOG = LoggerFactory.getLogger(Plan.class);

  PlanType getPlanType();

  List<ColumnType> getOutTypes();

  Modifier getPlanModifier();

  List<Modifier> getOutModifiers();

  List<Plan> getChildren();

  String getTableName();

  void setTableName(String name);

  List<Expression> getSelectExps();

  void setSelectExps(List<Expression> selectExps);

  List<Expression> getWhereExps();

  void setWhereExps(List<Expression> whereExps);

  List<Expression> getAggExps();

  void setAggExps(List<Expression> aggExps);

  boolean hasAgg();

  List<Integer> getGroups();

  void setGroups(List<Integer> groups);

  List<Collation> getOrders();

  void setOrders(List<Collation> orders);

  int getFetch();

  void setFetch(int fetch);

  int getOffset();

  void setOffset(int offset);

  JoinCondition getJoinCond();

  void setJoinInfo(JoinCondition joinInfo);

  TaskInfo getTaskInfo();

  DataSet implement(PlanImplementor implementor);

  Plan rewrite(Rewriter rewriter);

  Schema getOutSchema();

  List<Expression> getOutExpressions();

  String toString();

  public static Plan fromProto(QueryPlanProto proto) {
    switch (proto.getType()) {
      case LEAF:
        return LeafPlan.fromProto(proto);
      case UNARY:
        return UnaryPlan.fromProto(proto);
      case BINARY:
        return BinaryPlan.fromProto(proto);
      case EMPTY:
        return EmptyPlan.fromProto(proto);
      default:
        LOG.error("Not support converting plan type {} into protocolbuffer", proto.getType());
        throw new RuntimeException("Unsupport plan type");
    }
  }
}
