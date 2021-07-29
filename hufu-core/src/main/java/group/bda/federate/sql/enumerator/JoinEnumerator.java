package group.bda.federate.sql.enumerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.calcite.linq4j.Enumerator;

import group.bda.federate.data.Row;
import group.bda.federate.sql.join.FedSpatialJoinInfo;


public class JoinEnumerator implements Enumerator<Row> {
  private Enumerator<Row> leftEnumerator;
  private Function<Row, Enumerator<Row>> matchFunction;
  private Enumerator<Row> currentRightEnumerator;
  private Row currentLeft;
  private Row currentRight;
  private Row current;
  private int rowSize;
  // left/right column index to result column index
  private final Map<Integer, Integer> leftIndex;
  private final Map<Integer, Integer> rightIndex;

  public JoinEnumerator(Enumerator<Row> leftEnumerator, FedSpatialJoinInfo joinInfo, List<Integer> projects, Function<Row, Enumerator<Row>> matchFunction) {
    this.leftEnumerator = leftEnumerator;
    this.currentRightEnumerator = RowEnumerator.emptyEnumerator();
    this.matchFunction = matchFunction;
    this.leftIndex = new HashMap<>();
    this.rightIndex = new HashMap<>();
    this.rowSize = projects.size();
    for (int i = 0; i < projects.size(); ++i) {
      if (projects.get(i) < joinInfo.getLeftSize()) {
        leftIndex.put(projects.get(i), i);
      } else {
        rightIndex.put(projects.get(i) - joinInfo.getLeftSize(), i);
      }
    }
  }

  @Override
  public Row current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    if (currentRightEnumerator.moveNext()) {
      currentRight = currentRightEnumerator.current();
    } else if (leftEnumerator.moveNext()) {
      // update right enumerator
      currentLeft = leftEnumerator.current();
      currentRightEnumerator = matchFunction.apply(currentLeft);
      return moveNext();
    } else {
      return false;
    }
    current = mergeRow(currentLeft, currentRight);
    return true;
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("unsupported operation");
  }

  @Override
  public void close() {
    // do nothing
  }

  public Row mergeRow(Row left, Row right) {
    Row.RowBuilder builder = Row.newBuilder(rowSize);
    for (Entry<Integer, Integer> entry : leftIndex.entrySet()) {
      builder.set(entry.getValue(), left.getObject(entry.getKey()));
    }
    for (Entry<Integer, Integer> entry : rightIndex.entrySet()) {
      builder.set(entry.getValue(), right.getObject(entry.getKey()));
    }
    return builder.build();
  }
}
