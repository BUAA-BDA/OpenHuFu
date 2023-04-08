package com.hufudb.openhufu.plan;

import java.util.List;
import com.hufudb.openhufu.implementor.PlanImplementor;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinCondition;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Collation;
import com.hufudb.openhufu.proto.OpenHuFuPlan.TaskInfo;


/**
 * Base Plan which all methods are not supported
 */
public abstract class BasePlan implements Plan {

  public List<Plan> getChildren() {
    LOG.error("not support getChildren");
    throw new UnsupportedOperationException();
  }

  public String getTableName() {
    LOG.error("not support getTableName");
    throw new UnsupportedOperationException();
  }

  public void setTableName(String name) {
    LOG.error("not support setTableName");
    throw new UnsupportedOperationException();
  }

  public List<Expression> getSelectExps() {
    LOG.error("not support getSelectExps");
    throw new UnsupportedOperationException();
  }

  public void setSelectExps(List<Expression> selectExps) {
    LOG.error("not support setSelectExps");
    throw new UnsupportedOperationException();
  }

  public List<Expression> getWhereExps() {
    LOG.error("not support getWhereExps");
    throw new UnsupportedOperationException();
  }

  public void setWhereExps(List<Expression> whereExps) {
    LOG.error("not support setWhereExps");
    throw new UnsupportedOperationException();
  }

  public List<Expression> getAggExps() {
    LOG.error("not support getAggExps");
    throw new UnsupportedOperationException();
  }

  public void setAggExps(List<Expression> aggExps) {
    LOG.error("not support setAggExps");
    throw new UnsupportedOperationException();
  }

  public boolean hasAgg() {
    return false;
  }

  public List<Integer> getGroups() {
    LOG.error("not support getGroups");
    throw new UnsupportedOperationException();
  }

  public void setGroups(List<Integer> groups) {
    LOG.error("not support setGroups");
    throw new UnsupportedOperationException();
  }

  public List<Collation> getOrders() {
    LOG.error("not support getOrders");
    throw new UnsupportedOperationException();
  }

  public void setOrders(List<Collation> orders) {
    LOG.error("not support setOrders");
    throw new UnsupportedOperationException();
  }

  public int getFetch() {
    LOG.error("not support getFetch");
    throw new UnsupportedOperationException();
  }

  public void setFetch(int fetch) {
    LOG.error("not support setFetch");
    throw new UnsupportedOperationException();
  }

  public int getOffset() {
    LOG.error("not support getOffset");
    throw new UnsupportedOperationException();
  }

  public void setOffset(int offset) {
    LOG.error("not support setOffset");
    throw new UnsupportedOperationException();
  }

  public JoinCondition getJoinCond() {
    LOG.error("not support getJoinInfo");
    throw new UnsupportedOperationException();
  }

  public void setJoinInfo(JoinCondition joinInfo) {
    LOG.error("not support setJoinInfo");
    throw new UnsupportedOperationException();
  }

  public TaskInfo getTaskInfo() {
    LOG.error("not support getTaskInfo");
    throw new UnsupportedOperationException();
  }

  public DataSet implement(PlanImplementor implementor) {
    LOG.error("not support implement");
    throw new UnsupportedOperationException();
  }
}
