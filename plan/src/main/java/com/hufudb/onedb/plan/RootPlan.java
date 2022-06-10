package com.hufudb.onedb.plan;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.implementor.PlanImplementor;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.PlanType;
import com.hufudb.onedb.rewriter.Rewriter;

public class RootPlan extends BasePlan {
  private static final AtomicLong counter = new AtomicLong(0);

  final long id;
  Plan child;

  public RootPlan() {
    counter.compareAndSet(Long.MAX_VALUE, 0);
    id = counter.addAndGet(1);
  }

  public long getPlanId() {
    return id;
  }

  public Plan getChild() {
    return child;
  }

  public void setChild(Plan child) {
    this.child = child;
  }

  @Override
  public PlanType getPlanType() {
    return PlanType.ROOT;
  }

  @Override
  public List<Expression> getOutExpressions() {
    return ImmutableList.of();
  }

  @Override
  public List<ColumnType> getOutTypes() {
    return ImmutableList.of();
  }

  @Override
  public Modifier getPlanModifier() {
    return child.getPlanModifier();
  }

  @Override
  public List<Modifier> getOutModifiers() {
    return child.getOutModifiers();
  }

  @Override
  public List<Plan> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public DataSet implement(PlanImplementor implementor) {
    return implementor.implement(child);
  }

  @Override
  public Plan rewrite(Rewriter rewriter) {
    this.child = child.rewrite(rewriter);
    return rewriter.rewriteRoot(this);
  }

  @Override
  public Schema getOutSchema() {
    return child.getOutSchema();
  }
};
