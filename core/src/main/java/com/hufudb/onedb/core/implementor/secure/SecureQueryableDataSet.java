package com.hufudb.onedb.core.implementor.secure;

import java.util.List;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.data.DataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

public class SecureQueryableDataSet implements QueryableDataSet {

  final OneDBClient client;

  public SecureQueryableDataSet(OneDBClient client) {
    this.client = client;
  }

  @Override
  public Header getHeader() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getRowCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void addRow(Row row) {
    // TODO Auto-generated method stub
  }

  @Override
  public void addRows(List<Row> rows) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void mergeDataSet(DataSet dataSet) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public List<Row> getRows() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Row current() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean moveNext() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public List<FieldType> getTypeList() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryableDataSet join(OneDBImplementor implementor, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryableDataSet filter(OneDBImplementor implementor, List<OneDBExpression> filters) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryableDataSet project(OneDBImplementor implementor, List<OneDBExpression> projects) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryableDataSet aggregate(OneDBImplementor implementor, List<Integer> groups,
      List<OneDBExpression> aggs, List<FieldType> inputTypes) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryableDataSet sort(OneDBImplementor implementor, List<String> orders) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryableDataSet limit(int offset, int fetch) {
    // TODO Auto-generated method stub
    return null;
  }
  
}
