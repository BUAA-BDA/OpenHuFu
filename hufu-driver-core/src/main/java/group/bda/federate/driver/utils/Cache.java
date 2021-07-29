package group.bda.federate.driver.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.rpc.FederateService.CompareShare;
import group.bda.federate.rpc.FederateService.CompareShareMatrix;
import group.bda.federate.rpc.FederateService.CompareShareVector;
import group.bda.federate.rpc.FederateService.DistanceJoinRequest;

public class Cache {
  private static final Logger LOG = LogManager.getLogger(Cache.class);
  private int k;
  private DistanceDataSet dataSet;
  private int id;
  private List<String> endpoints;
  private int[][] shareMatrix; // nk x nk
  private String[] values; // nk
  private List<ComparableRow<Double>> rows;
  private List<Integer> qValueList; // endpoint to q

  // join
  private Map<Long, List<Long>> intersectMap;
  private DistanceJoinRequest distanceJoinRequest;

  public Cache() {
    qValueList = new Vector<>();
  }

  public Cache(int k, List<ComparableRow<Double>> rows) {
    this.k = k;
    this.rows = rows;
    qValueList = new Vector<>();
  }

  public Cache(int k, DistanceDataSet dataSet) {
    this.k = k;
    this.dataSet = dataSet;
    qValueList = new Vector<>();
  }

  public DistanceDataSet getDataSet() {
    return dataSet;
  }

  public Cache(Map<Long, List<Long>> intersectMap, DistanceJoinRequest request) {
    this.intersectMap = intersectMap;
    this.distanceJoinRequest = request;
  }

  public void initShareMatrix(int k, int n) {
    // System.out.printf("init matrix with [%d][%d]\n", k, n);
    shareMatrix = new int[n * k][n * k];
    values = new String[n * k];
  }

  public boolean hasMatrix() {
    return shareMatrix != null;
  }

  // public void setShareVector(int localIndex, int globalIndex, int share) {
  // if (shareVector == null) {
  // System.out.println("please init share vector first");
  // return;
  // }
  // shareVector[localIndex][globalIndex] = share;
  // }

  // public int getShareVector(int localIndex, int globalIndex) {
  // return shareVector[localIndex][globalIndex];
  // }

  public void setShareMatrix(int i, int j, int share) {
    // System.out.printf("set matrix [%d][%d] with [%d]\n", i, j, share);
    shareMatrix[i][j] = share;
  }

  public int getShareMatrix(int i, int j, int share) {
    return shareMatrix[i][j];
  }

  public void setValue(int i, String value) {
    // System.out.printf("set values[%d] with [%s]\n", i, value);
    values[i] = value;
  }

  public String getValue(int i) {
    return values[i];
  }

  public void setEndpoints(List<String> endpoints) {
    this.endpoints = endpoints;
  }

  public void setRows(List<ComparableRow<Double>> rows) {
    this.rows = rows;
  }

  public void setQValue(int qValue) {
    qValueList.add(qValue);
  }

  public boolean isQValueComplete(int clientNum) {
    return qValueList.size() == clientNum;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public int getK() {
    return k;
  }

  public int getEndpointCount() {
    return endpoints.size();
  }

  public String getEndpoint(int i) {
    return endpoints.get(i);
  }

  public Map<Long, List<Long>> getIntersectMap() {
    return intersectMap;
  }

  public DistanceJoinRequest getDistanceJoinArgs() {
    return distanceJoinRequest;
  }

  public int getRangeCount(double radius) {
    int targetIdx = binarySearch(radius, 0, rows.size());
    if (rows.get(targetIdx).compareKey == radius) {
      return targetIdx + 1;
    } else {
      return targetIdx;
    }
  }

  public List<ComparableRow<Double>> getRangeResult(double radius) {
    return rows.subList(0, getRangeCount(radius));
  }

  public int binarySearch(double target, int left, int right) {
    while (right > left) {
      int mid = left + (right - left) / 2;
      if (rows.get(mid).compareKey == target) {
        while (mid + 1 < rows.size() && rows.get(mid + 1).compareKey == target) {
          mid++;
        }
        return mid;
      } else if (rows.get(mid).compareKey > target) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    return right;
  }

  public List<ComparableRow<Double>> getRows() {
    return rows;
  }

  public int calSum() {
    int sum = 0;
    for (int q : qValueList) {
      sum += q;
    }
    qValueList.clear();
    return sum;
  }

  // @Precondition: i < j
  public boolean greater(int i, int j) {
    return (shareMatrix[i][j] ^ shareMatrix[j][i]) == 1;
  }

  public CompareShareMatrix getMatrix(String uuid) {
    CompareShareMatrix.Builder mBuilder = CompareShareMatrix.newBuilder();
    mBuilder.setUuid(uuid);
    for (int i = 0; i < shareMatrix.length; ++i) {
      CompareShareVector.Builder vBuilder = CompareShareVector.newBuilder();
      vBuilder.setId(i);
      vBuilder.setValue(values[i]);
      for (int j = 0; j < shareMatrix[i].length; ++j) {
        vBuilder.addShare(CompareShare.newBuilder().setIdx(j).setShare(shareMatrix[i][j]));
      }
      mBuilder.addShares(vBuilder);
    }
    return mBuilder.build();
  }

  public List<String> calGlobalKnn() {
    List<String> result = new ArrayList<String>();
    int n = endpoints.size();
    int[] cursors = new int[n];
    int rest = k;
    while (rest > 0) {
      int minIdx = cursors[0];
      for (int i = 1; i < n; ++i) {
        if (greater(minIdx, i * k + cursors[i])) {
          minIdx = i * k + cursors[i];
        }
      }
      result.add(values[minIdx]);
      cursors[minIdx / k]++;
      rest--;
      LOG.debug("get {} in global knn", values[minIdx]);
    }
    LOG.debug("cal global knn finish");
    return result;
  }

}