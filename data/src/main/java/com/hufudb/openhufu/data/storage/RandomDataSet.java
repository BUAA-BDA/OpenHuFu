package com.hufudb.openhufu.data.storage;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.proto.OpenHuFuData;
import java.security.SecureRandom;
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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

/*
 * Used for security union, insert fake record to dataset
 */
public class RandomDataSet {

  private static final GeometryFactory geoFactory = new GeometryFactory();
  private static final double RANDOM_SET_SCALE = 0.5;
  private static final double EPS = 1.0;
  private static final int RANDOM_SET_OFFSET = 10;
  private static final LaplaceDistribution lap = new LaplaceDistribution(0, 1 / EPS);
  private static final Random random = new SecureRandom();
  private final Schema schema;
  private final List<ArrayRow> originRows;
  private final int originSize;
  private final int resultSize;
  private final List<ArrayRow> randomRows;
  private final Map<Object, List<ArrayRow>> randomRowMap;

  public RandomDataSet(DataSet source) {
    this.schema = source.getSchema();
    this.originRows = ArrayDataSet.materialize(source).rows;
    this.originSize = originRows.size();
    this.randomRows = new ArrayList<>();
    this.randomRowMap = new HashMap<>();
    int size = (int) Math.ceil((double) originSize * RANDOM_SET_SCALE + lap.sample());
    this.resultSize = size > 0 ? size : (int) Math.abs(lap.sample());
    if (originSize == 0) {
      init(this::getRandomValue);
    } else {
      init(this::getRandomValueFromData);
    }
    this.mix();
  }

  private void init(Function<Integer, Object> randomFunc) {
    final int headerSize = schema.size();
    if (headerSize == 0) {
      return;
    }
    for (int i = 0; i < resultSize; ++i) {
      Object[] objects = new Object[headerSize];
      Object key = randomFunc.apply(0);
      objects[0] = key;
      for (int j = 1; j < headerSize; ++j) {
        Object value = randomFunc.apply(j);
        objects[j] = value;
      }
      ArrayRow row = new ArrayRow(objects);
      randomRows.add(row);
      recordRandomRow(key, row);
    }
  }

  private void recordRandomRow(Object key, ArrayRow value) {
    if (randomRowMap.containsKey(key)) {
      randomRowMap.get(key).add(value);
    } else {
      randomRowMap.put(key, new LinkedList<>(Arrays.asList(value)));
    }
  }

  private void mix() {
    //todo index insert for ArrayList may be slow
    for (ArrayRow row : originRows) {
      int idx = (int) Math.ceil(random.nextDouble() * randomRows.size());
      randomRows.add(idx, row);
    }
  }

  public ArrayDataSet getRandomSet() {
    return new ArrayDataSet(schema, randomRows);
  }

  public ArrayDataSet removeRandom(DataSet dataSet) {
    List<ArrayRow> newRows = new ArrayList<>();
    for (ArrayRow row : ArrayDataSet.materialize(dataSet).rows) {
      Object key = row.get(0);
      List<ArrayRow> rows = randomRowMap.get(key);
      if (rows == null || rows.isEmpty()) {
        newRows.add(row);
        continue;
      }
      int idx = rows.indexOf(row);
      if (idx == -1) {
        newRows.add(row);
      } else {
        rows.remove(idx);
      }
    }
    return new ArrayDataSet(schema, newRows);
  }

  private Object getRandomValueFromData(int columnIndex) {
    OpenHuFuData.ColumnType type = schema.getType((columnIndex));
    int r = (int) (random.nextDouble() * originSize);
    switch (type) {
      case BYTE:
        return (byte) originRows.get(r).get(columnIndex) + (byte) lap.sample();
      case SHORT:
        return (short) originRows.get(r).get(columnIndex) + (short) lap.sample();
      case INT:
        return (int) originRows.get(r).get(columnIndex) + (int) lap.sample();
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        return (long) originRows.get(r).get(columnIndex) + (long) lap.sample();
      case FLOAT:
        return (float) originRows.get(r).get(columnIndex) + (float) lap.sample();
      case DOUBLE:
        return (double) originRows.get(r).get(columnIndex) + lap.sample();
      case BOOLEAN:
        return lap.sample() > 0.0;
      case GEOMETRY:
        Point p = (Point) originRows.get(r).get(columnIndex);
        return geoFactory.createPoint(
            new Coordinate(p.getX() + lap.sample(), p.getX() + lap.sample()));
      case STRING:
        return originRows.get(r).get(columnIndex);
      default:
        throw new OpenHuFuException(ErrorCode.DATA_TYPE_NOT_SUPPORT, type);
    }
  }

  private Object getRandomValue(int columnIndex) {
    OpenHuFuData.ColumnType type = schema.getType((columnIndex));
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
      case GEOMETRY:
        return geoFactory.createPoint(new Coordinate(lap.sample(), lap.sample()));
      case STRING:
        return RandomStringUtils.randomAlphanumeric(RANDOM_SET_OFFSET); // NOSONAR
      default:
        throw new OpenHuFuException(ErrorCode.DATA_TYPE_NOT_SUPPORT, type);
    }
  }
}
