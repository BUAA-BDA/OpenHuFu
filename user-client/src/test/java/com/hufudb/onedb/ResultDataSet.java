package com.hufudb.onedb;

import com.csvreader.CsvReader;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Row;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultDataSet {
  private final List<Row> realAnswer;
  private final List<Row> output;

  public ResultDataSet() {
    realAnswer = new ArrayList<>();
    output = new ArrayList<>();
  }

  private int compare(Row row1, Row row2) {
    int compareResult = 0;
    for (int i = 0; i < row1.size(); i++) {
      compareResult = ((Comparable) row1.getObject(i)).compareTo((Comparable) row2.getObject(i));
      if (compareResult != 0) {
        return compareResult;
      }
    }
    return compareResult;
  }

  private void sort() {
    realAnswer.sort(this::compare);
    output.sort(this::compare);
  }

  public boolean compareWithOrder() {
    if (realAnswer.size() != output.size()) {
      return false;
    }
    for (int i = 0; i < realAnswer.size(); i++) {
      if (compare(realAnswer.get(i), output.get(i)) != 0) {
        return false;
      }
    }
    return true;
  }

  public boolean compareWithoutOrder() {
    sort();
    return compareWithOrder();
  }

  void addRealAnswer(String csvPath, List<FieldType> header) {
    ClassLoader classLoader = OneDBTest.class.getClassLoader();
    URL resource = classLoader.getResource(csvPath);
    try {
      ArrayList<String[]> csvFileList = new ArrayList<>();
      assert resource != null;
      CsvReader reader = new CsvReader(resource.getPath(), ',', StandardCharsets.UTF_8);
      reader.readHeaders();
      while (reader.readRecord()) {
        csvFileList.add(reader.getValues());
      }
      reader.close();
      for (int i = 0; i < csvFileList.size(); i++) {
        Row.RowBuilder builder = Row.newBuilder(csvFileList.get(0).length);
        for (int j = 0; j < csvFileList.get(0).length; j++) {
          switch (header.get(j)) {
            case BYTE:
              builder.set(j, Byte.valueOf(csvFileList.get(i)[j]));
              break;
            case SHORT:
              builder.set(j, Short.valueOf(csvFileList.get(i)[j]));
              break;
            case INT:
              builder.set(j, Integer.valueOf(csvFileList.get(i)[j]));
              break;
            case TIME:
            case DATE:
            case TIMESTAMP:
            case LONG:
              builder.set(j, Long.valueOf(csvFileList.get(i)[j]));
              break;
            case FLOAT:
              builder.set(j, Float.valueOf(csvFileList.get(i)[j]));
              break;
            case DOUBLE:
              builder.set(j, Double.valueOf(csvFileList.get(i)[j]));
              break;
            case STRING:
              builder.set(j, csvFileList.get(i)[j]);
              break;
            case BOOLEAN:
              builder.set(j, Boolean.valueOf(csvFileList.get(i)[j]));
              break;
            default:
              throw new RuntimeException("not support this type");
          }
        }
        addRealAnswer(builder.build());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  void addRealAnswer(Row row) {
    realAnswer.add(row);
  }

  void  addOutput(ResultSet rs){
    try {
      while (rs.next()) {
        Row.RowBuilder builder = Row.newBuilder(rs.getMetaData().getColumnCount());
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
          builder.set(i, rs.getObject(i + 1));
        }
        addOutput(builder.build());
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

  }

  void addOutput(Row row) {
    output.add(row);
  }
}
