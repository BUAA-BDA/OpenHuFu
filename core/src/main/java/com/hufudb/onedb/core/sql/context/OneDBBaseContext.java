package com.hufudb.onedb.core.sql.context;

import java.util.List;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import com.hufudb.onedb.core.table.OneDBTableSchema;
import com.hufudb.onedb.rpc.OneDBCommon.TaskInfoProto;


/*
 * Base context which all methods are not supported
 */
public abstract class OneDBBaseContext implements OneDBContext {
  @Override
  public OneDBContext getParent() {
    LOG.error("not support getParent");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setParent(OneDBContext parent) {
    LOG.error("not support setParent");
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OneDBContext> getChildren() {
    LOG.error("not support getChildren");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChildren(List<OneDBContext> children) {
    LOG.error("not support setChildren");
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateChild(OneDBContext newChild, OneDBContext oldChild) {
    LOG.error("not support updateChild");
    throw new UnsupportedOperationException();
  }

  @Override
  public String getTableName() {
    LOG.error("not support getTableName");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTableName(String name) {
    LOG.error("not support setTableName");
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OneDBExpression> getSelectExps() {
    LOG.error("not support getSelectExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSelectExps(List<OneDBExpression> selectExps) {
    LOG.error("not support setSelectExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OneDBExpression> getWhereExps() {
    LOG.error("not support getWhereExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWhereExps(List<OneDBExpression> whereExps) {
    LOG.error("not support setWhereExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OneDBExpression> getAggExps() {
    LOG.error("not support getAggExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAggExps(List<OneDBExpression> aggExps) {
    LOG.error("not support setAggExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasAgg() {
    return false;
  }

  @Override
  public List<Integer> getGroups() {
    LOG.error("not support getGroups");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setGroups(List<Integer> groups) {
    LOG.error("not support setGroups");
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OneDBOrder> getOrders() {
    LOG.error("not support getOrders");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOrders(List<OneDBOrder> orders) {
    LOG.error("not support setOrders");
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFetch() {
    LOG.error("not support getFetch");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFetch(int fetch) {
    LOG.error("not support setFetch");
    throw new UnsupportedOperationException();
  }

  @Override
  public int getOffset() {
    LOG.error("not support getOffset");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOffset(int offset) {
    LOG.error("not support setOffset");
    throw new UnsupportedOperationException();
  }

  @Override
  public OneDBJoinInfo getJoinInfo() {
    LOG.error("not support getJoinInfo");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setJoinInfo(OneDBJoinInfo joinInfo) {
    LOG.error("not support setJoinInfo");
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskInfoProto getTaskInfo() {
    LOG.error("not support getTaskInfo");
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryableDataSet implement(OneDBImplementor implementor) {
    LOG.error("not support implement");
    throw new UnsupportedOperationException();
  }
}
