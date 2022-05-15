package com.hufudb.onedb.core.data;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.hufudb.onedb.core.sql.expression.OneDBOpType;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sarg;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class SearchList {

  enum SearchType {
    STRING, INT, DOUBLE
  }

  private List<Pair<Comparable, Comparable>> searchList;
  private SearchType searchType;
  private ColumnType fieldType;

  public SearchList(ColumnType type, SearchType searchType,
      List<Pair<Comparable, Comparable>> searchList) {
    this.fieldType = type;
    this.searchType = searchType;
    this.searchList = searchList;
  }

  public SearchList(ColumnType type, Object value) {
    fieldType = type;
    switch (type) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        searchType = SearchType.INT;
        break;
      case FLOAT:
      case DOUBLE:
        searchType = SearchType.DOUBLE;
        break;
      case STRING:
        searchType = SearchType.STRING;
        break;
      default:
        throw new RuntimeException(
            "not support type including bool, time, date and point in search operation");
    }
    searchList = new ArrayList<>();
    ImmutableRangeSet rangeSet = (ImmutableRangeSet) ((Sarg) value).rangeSet;
    ImmutableSet<Range<Comparable>> ranges = rangeSet.asRanges();
    for (Range<Comparable> p : ranges) {
      switch (searchType) {
        case INT:
          searchList.add(new Pair<>(((BigDecimal) p.lowerEndpoint()).longValue(),
              ((BigDecimal) p.upperEndpoint()).longValue()));
          break;
        case DOUBLE:
          searchList.add(new Pair<>(((BigDecimal) p.lowerEndpoint()).doubleValue(),
              ((BigDecimal) p.upperEndpoint()).doubleValue()));
          break;
        case STRING:
          searchList.add((new Pair<>(((NlsString) p.lowerEndpoint()).getValue(),
              ((NlsString) p.upperEndpoint()).getValue())));
      }
    }
  }

  public ExpressionProto toProto() {
    ExpressionProto.Builder builder = ExpressionProto.newBuilder()
        .setOpType(OneDBOpType.LITERAL.ordinal()).setOutType(ColumnType.SARG.ordinal());
    for (Pair<Comparable, Comparable> p : searchList) {
      ExpressionProto.Builder lowerBuilder = ExpressionProto.newBuilder();
      ExpressionProto.Builder upperBuilder = ExpressionProto.newBuilder();
      switch (searchType) {
        case INT:
          lowerBuilder.setI64((Long) p.left).setOutType(fieldType.ordinal());
          upperBuilder.setI64((Long) p.right).setOutType(fieldType.ordinal());
          break;
        case DOUBLE:
          lowerBuilder.setF64((Double) p.left).setOutType(fieldType.ordinal());
          upperBuilder.setF64((Double) p.right).setOutType(fieldType.ordinal());
          break;
        case STRING:
          lowerBuilder.setStr((String) p.left).setOutType(fieldType.ordinal());
          upperBuilder.setStr((String) p.right).setOutType(fieldType.ordinal());
      }
      builder.addIn(lowerBuilder.build());
      builder.addIn(upperBuilder.build());
    }
    return builder.build();
  }

  private String object2String(Comparable obj) {
    if (fieldType == ColumnType.STRING) {
      return String.format("'%s'", obj.toString());
    } else {
      return obj.toString();
    }
  }

  public static SearchList fromProto(ExpressionProto proto) {
    List<ExpressionProto> protoList = proto.getInList();
    ColumnType type = ColumnType.of(protoList.get(0).getOutType());
    assert protoList.size() % 2 == 0;
    SearchType searchType;
    switch (type) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        searchType = SearchType.INT;
        break;
      case FLOAT:
      case DOUBLE:
        searchType = SearchType.DOUBLE;
        break;
      case STRING:
        searchType = SearchType.STRING;
        break;
      default:
        throw new RuntimeException(
            "not support type including bool, time, date and point in search operation");
    }
    List<Pair<Comparable, Comparable>> searchList = new ArrayList<>();
    for (int i = 0; i < protoList.size(); i += 2) {
      switch (searchType) {
        case INT:
          searchList.add(new Pair<>(protoList.get(i).getI64(), protoList.get(i + 1).getI64()));
          break;
        case DOUBLE:
          searchList.add(new Pair<>(protoList.get(i).getF64(), protoList.get(i + 1).getF64()));
          break;
        case STRING:
          searchList.add(new Pair<>(protoList.get(i).getStr(), protoList.get(i + 1).getStr()));
      }
    }
    return new SearchList(type, searchType, searchList);
  }

  public List<String> toSqlString() {
    List<String> inClause = new ArrayList<>();
    List<String> betweenAndClause = new ArrayList<>();
    for (Pair<Comparable, Comparable> p : searchList) {
      if (p.left.compareTo(p.right) == 0) {
        inClause.add(object2String(p.left));
      } else {
        betweenAndClause.add(
            String.format(" between %s and %s ", object2String(p.left), object2String(p.right)));
      }
    }
    String inClauseJoin = String.join(", ", inClause);
    List<String> searchClause = new ArrayList<>();
    if (inClause.size() > 0) {
      searchClause.add(String.format(" in (%s) ", inClauseJoin));
    }
    searchClause.addAll(betweenAndClause);
    return searchClause;
  }
}
