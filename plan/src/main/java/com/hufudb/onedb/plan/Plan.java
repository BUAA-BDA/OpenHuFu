package com.hufudb.onedb.plan;

import java.util.List;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.implementor.PlanImplementor;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Collation;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;
import com.hufudb.onedb.proto.OneDBPlan.QueryPlanProto;
import com.hufudb.onedb.proto.OneDBPlan.TaskInfo;
import com.hufudb.onedb.rewriter.Rewriter;
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
