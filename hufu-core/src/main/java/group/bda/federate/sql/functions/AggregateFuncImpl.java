package group.bda.federate.sql.functions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import group.bda.federate.client.FederateDBClient;
import group.bda.federate.data.DataSet;
import group.bda.federate.rpc.FederateService;
import group.bda.federate.security.secretsharing.ShamirSharing;
import group.bda.federate.sql.type.FederateFieldType;


public class AggregateFuncImpl {

  public static AggregateFunc getAggFunc(AggregateType aggType, FederateFieldType outType, List<Integer> inputs) {
    switch (aggType) {
      case COUNT:
        return new COUNT(inputs.get(0));
      case AVG:
        return new AVG(inputs.get(0), inputs.get(1), outType);
      case MIN:
        switch (outType) {
          case BYTE:
            return new MIN<>(inputs.get(0), Byte.MAX_VALUE);
          case SHORT:
            return new MIN<>(inputs.get(0), Short.MAX_VALUE);
          case INT:
            return new MIN<>(inputs.get(0), Integer.MAX_VALUE);
          case DATE:
          case TIME:
          case TIMESTAMP:
          case LONG:
            return new MIN<>(inputs.get(0), Long.MAX_VALUE);
          case FLOAT:
            return new MIN<>(inputs.get(0), Float.MAX_VALUE);
          case DOUBLE:
            return new MIN<>(inputs.get(0), Double.MAX_VALUE);
          default:
            throw new RuntimeException("can't translate " + aggType + " with type " + outType);
        }
      case MAX:
        switch (outType) {
          case BYTE:
            return new MAX<>(inputs.get(0), Byte.MIN_VALUE);
          case SHORT:
            return new MAX<>(inputs.get(0), Short.MIN_VALUE);
          case INT:
            return new MAX<>(inputs.get(0), Integer.MIN_VALUE);
          case DATE:
          case TIME:
          case TIMESTAMP:
          case LONG:
            return new MAX<>(inputs.get(0), Long.MIN_VALUE);
          case FLOAT:
            return new MAX<>(inputs.get(0), Float.MIN_VALUE);
          case DOUBLE:
            return new MAX<>(inputs.get(0), Double.MIN_VALUE);
          default:
            throw new RuntimeException("can't translate " + aggType + " with type " + outType);
        }
      case SUM:
        if (FederateFieldType.INTEGER.contains(outType)) {
          return new AggregateFuncImpl.SUM_INT(inputs.get(0), outType);
        } else if (FederateFieldType.REAL.contains(outType)) {
          return new AggregateFuncImpl.SUM_FLOAT(inputs.get(0), outType);
        } else {
          throw new RuntimeException("can't translate " + aggType + " with type " + outType);
        }
      default:
        throw new RuntimeException("can't translate " + aggType + "with type" + outType);
    }
  }

  static public class AVG implements AggregateFunc {
    COUNT c;
    AggregateFunc s;
    FederateFieldType type;

    public AVG(int sumIdx, int countIdx, FederateFieldType type) {
      this.c = new COUNT(countIdx);
      if (FederateFieldType.INTEGER.contains(type)) {
        this.s = new SUM_INT(sumIdx, type);
      } else if (FederateFieldType.REAL.contains(type)) {
        this.s = new SUM_FLOAT(sumIdx, type);
      }
      this.type = type;
    }

    public void addRow(DataSet.DataRow row) {
      c.addRow(row);
      s.addRow(row);
    }

    @Override
    public void ShamirCount(String aggUuid, Map<FederateDBClient, String> tableClients, ExecutorService executorService) {
      c.ShamirCount(aggUuid, tableClients, executorService);
      s.ShamirCount(aggUuid, tableClients, executorService);
    }

    public Object result() {
      switch (type) {
        case BYTE:
          return ((Number) (((Number) s.result()).byteValue() / c.result())).byteValue();
        case SHORT:
          return ((Number) (((Number) s.result()).shortValue() / c.result())).shortValue();
        case INT:
          return ((Number) (((Number) s.result()).intValue() / c.result())).intValue();
        case LONG:
          return ((Number) (((Number) s.result()).longValue() / c.result())).longValue();
        case FLOAT:
          return ((Number) (((Number) s.result()).floatValue() / c.result())).floatValue();
        case DOUBLE:
          return ((Number) (((Number) s.result()).doubleValue() / c.result())).doubleValue();
        default:
          throw new RuntimeException("can't cal sum of type " + type);
      }
    }
  }

  static public class COUNT implements AggregateFunc {
    long count;
    int countIdx;

    public COUNT(int countIdx) {
      this.count = 0;
      this.countIdx = countIdx;
    }

    public void addRow(DataSet.DataRow row) {
      count += row.getLong(countIdx);
    }

    @Override
    public void ShamirCount(String aggUuid, Map<FederateDBClient, String> tableClients, ExecutorService executorService) {
      count = Math.round(ShamirSharing.shamirCount(tableClients,
              FederateService.PrivacyCountRequest.newBuilder().setCacheUuid(aggUuid).setColumnId(countIdx), executorService));
    }


    public Long result() {
      return count;
    }
  }

  static public class MAX<T extends Comparable<T>> implements AggregateFunc {
    T max;
    int maxIdx;

    public MAX(int maxIdx, T init) {
      this.maxIdx = maxIdx;
      this.max = init;
    }

    @Override
    public void addRow(DataSet.DataRow row) {
      T tmp = (T) row.get(maxIdx);
      if (max.compareTo(tmp) < 0) {
        max = tmp;
      }
    }

    @Override
    public void ShamirCount(String aggUuid, Map<FederateDBClient, String> tableClients, ExecutorService executorService) {
      throw new RuntimeException("do not support shamir sharing for max");
    }

    @Override
    public Object result() {
      return max;
    }
  }

  static public class MIN<T extends Comparable<T>> implements AggregateFunc {
    T min;
    int minIdx;

    public MIN(int maxIdx, T init) {
      this.minIdx = maxIdx;
      this.min = init;
    }

    @Override
    public void addRow(DataSet.DataRow row) {
      T tmp = (T) row.get(minIdx);
      if (min.compareTo(tmp) > 0) {
        min = tmp;
      }
    }

    @Override
    public void ShamirCount(String aggUuid, Map<FederateDBClient, String> tableClients, ExecutorService executorService) {
      throw new RuntimeException("do not support shamir sharing for min");
    }

    @Override
    public Object result() {
      return min;
    }
  }

  static public class SUM_INT implements AggregateFunc {
    Long sum;
    int sumIdx;
    FederateFieldType type;

    public SUM_INT(int sumIdx, FederateFieldType type) {
      this.sumIdx = sumIdx;
      this.sum = 0L;
      this.type = type;
    }

    @Override
    public void addRow(DataSet.DataRow row) {
      sum += row.getLong(sumIdx);
    }

    @Override
    public void ShamirCount(String aggUuid, Map<FederateDBClient, String> tableClients, ExecutorService executorService) {
      sum = Math.round(ShamirSharing.shamirCount(tableClients,
              FederateService.PrivacyCountRequest.newBuilder().setCacheUuid(aggUuid).setColumnId(sumIdx), executorService));
    }

    @Override
    public Object result() {
      switch (type) {
        case BYTE:
          return ((Number) sum).byteValue();
        case SHORT:
          return ((Number) sum).shortValue();
        case INT:
          return ((Number) sum).intValue();
        default:
          return ((Number) sum).longValue();
      }
    }
  }

  static public class SUM_FLOAT implements AggregateFunc {
    double sum;
    int sumIdx;
    FederateFieldType type;

    public SUM_FLOAT(int sumIdx, FederateFieldType type) {
      this.sumIdx = sumIdx;
      this.sum = 0;
      this.type = type;
    }

    @Override
    public void addRow(DataSet.DataRow row) {
      sum += row.getDouble(sumIdx);
    }

    @Override
    public void ShamirCount(String aggUuid, Map<FederateDBClient, String> tableClients, ExecutorService executorService) {
      sum = ShamirSharing.shamirCount(tableClients,
              FederateService.PrivacyCountRequest.newBuilder().setCacheUuid(aggUuid).setColumnId(sumIdx), executorService);
    }

    @Override
    public Object result() {
      switch (type) {
        case FLOAT:
          return ((Number) sum).floatValue();
        default:
          return ((Number) sum).doubleValue();
      }
    }
  }
}
