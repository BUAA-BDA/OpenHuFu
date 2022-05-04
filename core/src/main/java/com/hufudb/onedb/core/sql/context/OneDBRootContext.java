package com.hufudb.onedb.core.sql.context;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.rewriter.OneDBRewriter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

public class OneDBRootContext extends OneDBBaseContext {
  private static final AtomicLong counter = new AtomicLong(0);

  final long id;
  OneDBContext child;

  public OneDBRootContext() {
    counter.compareAndSet(Long.MAX_VALUE, 0);
    id = counter.addAndGet(1);
  }

  public long getContextId() {
    return id;
  }

  public OneDBContext getChild() {
    return child;
  }

  public void setChild(OneDBContext child) {
    this.child = child;
  }

  @Override
  public OneDBContextType getContextType() {
    return OneDBContextType.ROOT;
  }

  @Override
  public List<OneDBExpression> getOutExpressions() {
    return ImmutableList.of();
  }

  @Override
  public List<FieldType> getOutTypes() {
    return ImmutableList.of();
  }

  @Override
  public Level getContextLevel() {
    return child.getContextLevel();
  }

  @Override
  public List<Level> getOutLevels() {
    return child.getOutLevels();
  }

  @Override
  public List<OneDBContext> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public void setChildren(List<OneDBContext> children) {
    assert children.size() == 1;
    child = children.get(0);
  }

  @Override
  public void updateChild(OneDBContext newChild, OneDBContext oldChild) {
    child = newChild;
  }

  @Override
  public QueryableDataSet implement(OneDBImplementor implementor) {
    return child.implement(implementor);
  }

  @Override
  public OneDBContext rewrite(OneDBRewriter rewriter) {
    this.child = child.rewrite(rewriter);
    return rewriter.rewriteRoot(this);
  }

  @Override
  public Set<OwnerClient> getOwners() {
    return child.getOwners();
  }
};
