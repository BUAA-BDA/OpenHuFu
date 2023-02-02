package com.hufudb.openhufu.plan;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.implementor.PlanImplementor;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.PlanType;
import com.hufudb.openhufu.rewriter.Rewriter;

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

  @Override
  public String toString() {
    return "RootPlan{" + '\n' +
        "\tid=" + id + "\n" +
        "\tchild->"+ child.toString().replace("\n", "\n\t") + "\n" +
        '}';
  }
};
