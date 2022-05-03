package com.hufudb.onedb.core.implementor.secure;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

public class SecureQueryableDataSet implements QueryableDataSet {
  long queryId = 0;
  final OneDBClient client;
  AtomicBoolean startReveal;
  AtomicBoolean isRevealed;
  BasicDataSet result;
  Lock lock;
  Condition revealCondition;

  public SecureQueryableDataSet(Header header, OneDBClient client) {
    this.client = client;
    this.startReveal = new AtomicBoolean(false);
    this.isRevealed = new AtomicBoolean(false);
    this.result = BasicDataSet.of(header);
    this.lock = new ReentrantLock();
    this.revealCondition = lock.newCondition();
  }

  @Override
  public Header getHeader() {
    return result.getHeader();
  }

  @Override
  public int getRowCount() {
    if (!isRevealed.get()) {
      reveal();
    }
    return result.getRowCount();
  }

  @Override
  public void addRow(Row row) {
    throw new UnsupportedOperationException("Secure queryable database not support add row");
  }

  @Override
  public void addRows(List<Row> rows) {
    throw new UnsupportedOperationException("Secure queryable database not support add rows");
  }

  @Override
  public void mergeDataSet(DataSet dataSet) {
    throw new UnsupportedOperationException("Secure queryable database not support merge dataset");
  }

  @Override
  public List<Row> getRows() {
    if (!isRevealed.get()) {
      reveal();
    }
    return result.getRows();
  }

  @Override
  public Row current() {
    if (!isRevealed.get()) {
      reveal();
    }
    return result.current();
  }

  @Override
  public boolean moveNext() {
    if (!isRevealed.get()) {
      reveal();
    }
    return result.moveNext();
  }

  @Override
  public void reset() {
    if (!isRevealed.get()) {
      reveal();
    }
    result.reset();
  }

  @Override
  public void close() {
    result.close();
  }

  @Override
  public List<FieldType> getTypeList() {
    return result.getHeader().getTypeList();
  }

  @Override
  public QueryableDataSet join(OneDBImplementor implementor, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    return null;
  }

  @Override
  public QueryableDataSet filter(OneDBImplementor implementor, List<OneDBExpression> filters) {
    throw new UnsupportedOperationException("Secure queryable database not support filter");
  }

  @Override
  public QueryableDataSet project(OneDBImplementor implementor, List<OneDBExpression> projects) {
    throw new UnsupportedOperationException("Secure queryable database not support project");
  }

  @Override
  public QueryableDataSet aggregate(OneDBImplementor implementor, List<Integer> groups,
      List<OneDBExpression> aggs, List<FieldType> inputTypes) {
    return null;
  }

  @Override
  public QueryableDataSet sort(OneDBImplementor implementor, List<String> orders) {
    throw new UnsupportedOperationException("Secure queryable database not support sort");
  }

  @Override
  public QueryableDataSet limit(int offset, int fetch) {
    return null;
  }

  // reveal
  void reveal() {
    try {
      if (startReveal.getAndSet(true)) {
        revealCondition.wait();
      }
      if (!isRevealed.get()) {
        // do something slow: send secure request to owners and get result
        isRevealed.set(true);
        revealCondition.notifyAll();
      }
    } catch (Exception e) {
      LOG.error("error when reveal secure dataset: {}", e.getMessage());
      e.printStackTrace();
    }
  }
}
