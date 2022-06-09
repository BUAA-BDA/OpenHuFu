package com.hufudb.onedb.plan;

import java.util.List;
import com.hufudb.onedb.implementor.PlanImplementor;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.Collation;
import com.hufudb.onedb.proto.OneDBPlan.TaskInfo;


/**
 * Base Plan which all methods are not supported
 */
public abstract class BasePlan implements Plan {

  @Override
  public List<Plan> getChildren() {
    LOG.error("not support getChildren");
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
  public List<Expression> getSelectExps() {
    LOG.error("not support getSelectExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSelectExps(List<Expression> selectExps) {
    LOG.error("not support setSelectExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Expression> getWhereExps() {
    LOG.error("not support getWhereExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWhereExps(List<Expression> whereExps) {
    LOG.error("not support setWhereExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Expression> getAggExps() {
    LOG.error("not support getAggExps");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAggExps(List<Expression> aggExps) {
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
  public List<Collation> getOrders() {
    LOG.error("not support getOrders");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOrders(List<Collation> orders) {
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
  public JoinCondition getJoinCond() {
    LOG.error("not support getJoinInfo");
    throw new UnsupportedOperationException();
  }

  @Override
  public void setJoinInfo(JoinCondition joinInfo) {
    LOG.error("not support setJoinInfo");
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskInfo getTaskInfo() {
    LOG.error("not support getTaskInfo");
    throw new UnsupportedOperationException();
  }

  @Override
  public DataSet implement(PlanImplementor implementor) {
    LOG.error("not support implement");
    throw new UnsupportedOperationException();
  }
}
