package group.bda.federate.data;

import edu.alibaba.mpc4j.crypto.fhe.Ciphertext;
import java.util.ArrayList;
import java.util.List;

public class PointDataSet extends StreamDataSet {
  final private DataSet dataSet;
  private List<Double> x;
  private List<Double> y;
  private boolean cached;
  private List<Ciphertext> cipherDist;
  private int rowCount;

  public PointDataSet(DataSet dataSet) {
    this(dataSet, new ArrayList<>(), new ArrayList<>());
  }

  public PointDataSet(DataSet dataSet, List<Double> x, List<Double> y) {
    this(dataSet, false, x, y, new ArrayList<>());
  }

  public PointDataSet(DataSet dataSet, boolean cached, List<Double> x, List<Double> y, List<Ciphertext> cipherDist) {
    super(dataSet.header);
    assert x.size() == y.size();
    this.dataSet = dataSet;
    this.x = x;
    this.y = y;
    this.cached = cached;
    this.cipherDist = cipherDist;
    this.rowCount = x.size();
  }

  public boolean getCached() {
    return cached;
  }

  public void setCached(boolean cached) {
    this.cached = cached;
  }

  public DataSet getDataSet() {
    return dataSet;
  }

  @Override
  public int getRowCount() {
    return rowCount;
  }

  public void addRow(Row row) {
    this.dataSet.rows.add(row);
    rowCount++;
  }

  public void addRows(List<Row> rows) {
    dataSet.rows.addAll(rows);
    rowCount += rows.size();
  }

  @Override
  public void addDataSet(DataSet dataSet) {
    this.dataSet.rows.addAll(dataSet.rows);
  }

  @Override
  public void close() {
    // do nothing
  }

  public double[] getPoint(int index) {
    return new double[] {x.get(index), y.get(index)};
  }

  public void addPoint(double x, double y) {
    this.x.add(x);
    this.y.add(y);
  }

  public Ciphertext getCipherDist(int index) {
    return cipherDist.get(index);
  }

  public List<Ciphertext> getCipherDist() {
    return cipherDist;
  }

  public void addAll(Ciphertext[] cipherDist) {
    this.cipherDist.addAll(List.of(cipherDist));
  }

  public void addAll(List<Ciphertext> cipherDist) {
    this.cipherDist.addAll(cipherDist);
  }


  public void removePoint(int index) {
    x.remove(index);
    y.remove(index);
    cipherDist.remove(index);
    rowCount--;
  }

  public List<List<Row>> getDivided(int divide) {
    List<List<Row>> dataSets = new ArrayList<>();
    List<Row> rows = dataSet.rows;
    int size = rows.size();
    int head = 0;
    for (int i = 0; i < divide - 1; ++i) {
      dataSets.add(rows.subList(head, head + size / divide));
      head += size / divide;
    }
    dataSets.add(rows.subList(head, size));
    return dataSets;
  }
}
