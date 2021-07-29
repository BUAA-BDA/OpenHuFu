package group.bda.federate.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.distribution.LaplaceDistribution;

import group.bda.federate.config.FedSpatialConfig;
import group.bda.federate.sql.type.FederateFieldType;
import group.bda.federate.sql.type.Point;


public class RandomDataSet {
  final static LaplaceDistribution lap = new LaplaceDistribution(0, 1/ FedSpatialConfig.EPS_DP);
  final static Random random = new Random();
  final Header header;
  final String uuid;
  final List<Row> originRows;
  final int originSize;
  final int resultSize;
  final List<Row> randomRows;
  final Map<Object, List<Row>> randomRowMap;

  public RandomDataSet(DataSet dataSet) {
    this.header = dataSet.header;
    this.uuid = dataSet.uuid;
    this.originRows = dataSet.rows;
    this.originSize = dataSet.rows.size();
    this.randomRows = new ArrayList<>();
    this.randomRowMap = new HashMap<>();
    int size = (int) Math.ceil((double) dataSet.rowCount() * FedSpatialConfig.RANDOM_SET_SCALE + lap.sample());
    this.resultSize = size > 0 ? size : (int)Math.abs(lap.sample());
    if (dataSet.rows.size() == 0) {
      init(this::getRandomValue);
    } else {
      init(this::getRandomValueFromData);
    }
    this.mix();
  }

  public int size() {
    return resultSize;
  }

  private void init(Function<Integer, Object> randomFunc) {
    final int headerSize = header.size();
    if (headerSize == 0) {
      return;
    }
    for (int i = 0; i < resultSize; ++i) {
      Row.RowBuilder rowBuilder = Row.newBuilder(headerSize);
      Object key = randomFunc.apply(0);
      rowBuilder.set(0, key);
      for (int j = 1; j < headerSize; ++j) {
        Object value = randomFunc.apply(j);
        rowBuilder.set(j, value);
      }
      Row row = rowBuilder.build();
      randomRows.add(row);
      recordRandomRow(key, row);
    }
  }

  private void recordRandomRow(Object key, Row value) {
    if (randomRowMap.containsKey(key)) {
      randomRowMap.get(key).add(value);
    } else {
      randomRowMap.put(key, new LinkedList<>(Arrays.asList(value)));
    }
  }

  private void mix() {
    for (Row row : originRows) {
      int idx = (int) Math.ceil(random.nextDouble() * randomRows.size());
      randomRows.add(idx, row);
    }
  }

  public DataSet removeRandom(DataSet dataSet) {
    DataSet newDataSet = DataSet.newDataSet(header, dataSet.uuid);
    for (Row row : dataSet.rows) {
      Object key = row.getObject(0);
      List<Row> rows = randomRowMap.get(key);
      if (rows == null || rows.isEmpty()) {
        newDataSet.rows.add(row);
        continue;
      }
      int idx = rows.indexOf(row);
      if (idx == -1) {
        newDataSet.rows.add(row);
      } else {
        rows.remove(idx);
      }
    }
    return newDataSet;
  }

  public DataSet getRandomSet() {
    return DataSet.newDataSetUnsafe(header, randomRows, uuid);
  }

  // todo: optimize
  private Object getRandomValueFromData(int columnIndex) {
    FederateFieldType type = header.getType(columnIndex);
    int r = (int) (random.nextDouble() * originSize);
    switch (type) {
      case BYTE:
        return (byte) originRows.get(r).getObject(columnIndex) + (byte)lap.sample();
      case SHORT:
        return (short) originRows.get(r).getObject(columnIndex) + (short)lap.sample();
      case INT:
        return (int) originRows.get(r).getObject(columnIndex) + (int)lap.sample();
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        return (long) originRows.get(r).getObject(columnIndex) + (long)lap.sample();
      case FLOAT:
        return (float) originRows.get(r).getObject(columnIndex) + (float)lap.sample();
      case DOUBLE:
        return (double) originRows.get(r).getObject(columnIndex) + lap.sample();
      case BOOLEAN:
        return lap.sample() > 0.0;
      case POINT:
        Point p = (Point) originRows.get(r).getObject(columnIndex);
        return new Point(p.getX() + lap.sample(), p.getX() + lap.sample());
      case STRING:
        return originRows.get(r).getObject(columnIndex);
      default:
        return null;
    }
  }

  private Object getRandomValue(int columnIndex) {
    FederateFieldType type = header.getType(columnIndex);
    switch (type) {
      case BYTE:
        return (byte) lap.sample();
      case SHORT:
        return (short) lap.sample();
      case INT:
        return (int) lap.sample();
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        return (long) lap.sample();
      case FLOAT:
        return (float) lap.sample();
      case DOUBLE:
        return lap.sample();
      case BOOLEAN:
        return lap.sample() > 0.0;
      case POINT:
        return new Point(lap.sample(), lap.sample());
      case STRING:
        return RandomStringUtils.randomAlphanumeric(FedSpatialConfig.RANDOM_SET_OFFSET);
      default:
        return null;
    }
  }
}
