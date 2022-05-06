package com.hufudb.onedb.owner.implementor;

import java.util.List;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.plaintext.PlaintextCalculator;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.context.OneDBBinaryContext;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBContextType;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import com.hufudb.onedb.owner.OwnerService;
import com.hufudb.onedb.owner.implementor.join.HashEqualJoin;
import com.hufudb.onedb.rpc.Rpc;

public class OwnerSideImplementor implements OneDBImplementor {
  Rpc rpc;
  OwnerService ownerAdapter;

  public OwnerSideImplementor(Rpc rpc, OwnerService ownerAdapter) {
    this.rpc = rpc;
    this.ownerAdapter = ownerAdapter;
  }

  @Override
  public QueryableDataSet implement(OneDBContext context) {
    return context.implement(this);
  }

  @Override
  public QueryableDataSet aggregate(QueryableDataSet in, List<Integer> groups,
          List<OneDBExpression> aggs, List<FieldType> inputTypes) {
    return null;
  }

  @Override
  public QueryableDataSet binaryQuery(OneDBBinaryContext binary) {
    List<OneDBContext> children = binary.getChildren();
    assert children.size() == 2;
    OneDBContext left = children.get(0);
    OneDBContext right = children.get(1);
    QueryableDataSet in;
    if (left.getContextType().equals(OneDBContextType.PLACEHOLDER)) {
      // right
      in = right.implement(this);
    } else if (right.getContextType().equals(OneDBContextType.PLACEHOLDER)) {
      // left
      in = left.implement(this);
    } else {
      LOG.error("Not support two side on a single owner yet");
      throw new UnsupportedOperationException("Not support two side on a single owner yet");
    }
    Header leftHeader = OneDBContext.getOutputHeader(left);
    Header rightHeader = OneDBContext.getOutputHeader(right);
    Header outputHeader = Header.joinHeader(leftHeader, rightHeader);
    QueryableDataSet result = HashEqualJoin.apply(in, binary.getJoinInfo(), rpc, binary.getTaskInfo(), outputHeader);
    if (!binary.getSelectExps().isEmpty()) {
      result = result.project(this, binary.getSelectExps());
    }
    return result;
  }

  @Override
  public QueryableDataSet unaryQuery(OneDBUnaryContext unary) {
    return null;
  }

  // todo: change database adapter as plugin and implement this method
  @Override
  public QueryableDataSet leafQuery(OneDBLeafContext leaf) {
    Header header = OneDBContext.getOutputHeader(leaf);
    BasicDataSet dataSet = BasicDataSet.of(header);
    try {
      ownerAdapter.dbQuery(leaf, dataSet);
    } catch (Exception e) {
      LOG.error("Error when execute query on Database");
      e.printStackTrace();
    }
    return new OwnerQueryableDataSet(dataSet);
  }

  @Override
  public QueryableDataSet join(QueryableDataSet left, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    return null;
  }

  @Override
  public QueryableDataSet filter(QueryableDataSet in, List<OneDBExpression> filters) {
    return null;
  }

  @Override
  public QueryableDataSet project(QueryableDataSet in, List<OneDBExpression> projects) {
    return PlaintextCalculator.apply(in, projects);
  }

  @Override
  public QueryableDataSet sort(QueryableDataSet in, List<OneDBOrder> orders) {
    return null;
  }
}
