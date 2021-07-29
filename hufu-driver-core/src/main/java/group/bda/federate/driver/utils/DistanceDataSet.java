package group.bda.federate.driver.utils;

import java.util.List;

import group.bda.federate.data.DataSet;
import group.bda.federate.data.StreamDataSet;

public class DistanceDataSet {
  private final DataSet dataSet;
  private final List<Double> distances;

  public DistanceDataSet(DataSet dataSet, List<Double> distances) {
    this.dataSet = dataSet;
    this.distances = distances;
  }

  public int getRangeCount(double radius) {
    return binarySearch(radius, 0, distances.size());
  }

  public DataSet getRangeView(double radius) {
    return DataSet.rangeView(dataSet, 0, getRangeCount(radius));
  }

  public void getRangeView(double radius, StreamDataSet streamDataSet) {
    streamDataSet.addDataSet(DataSet.rangeView(dataSet, 0, getRangeCount(radius)));
  }

  public DataSet getDataSet() {
    return dataSet;
  }

  public int size() {
    return distances.size();
  }

  public double getDistance(int index) {
    return distances.get(index);
  }

  public int binarySearch(double target, int left, int right) {
    while (right > left) {
      int mid = left + (right - left) / 2;
      if (distances.get(mid) == target) {
        while (mid + 1 < distances.size() && distances.get(mid + 1) == target) {
          mid++;
        }
        return mid;
      } else if (distances.get(mid) > target) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    return right;
  }
}
