package com.hufudb.onedb.interpreter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.ArrayRow;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.ProtoDataSet;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import com.hufudb.onedb.proto.OneDBPlan.JoinType;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import org.junit.Test;

public class InterpreterTest {
  ProtoDataSet generatetestDataSet() {
    final Schema schema = Schema.newBuilder().add("A", ColumnType.INT, Modifier.PUBLIC)
        .add("B", ColumnType.DOUBLE, Modifier.PUBLIC).build();
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    ArrayRow.Builder builder = ArrayRow.newBuilder(2);
    builder.reset();
    builder.set(0, 3);
    builder.set(1, 4.2);
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 5);
    builder.set(1, 6.3);
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 7);
    builder.set(1, 8.4);
    dBuilder.addRow(builder.build());
    builder.reset();
    return dBuilder.build();
  }

  @Test
  public void testFilterDataSet() {
    DataSet source = generatetestDataSet();
    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression c0 = ExpressionFactory.createLiteral(ColumnType.INT, 5);
    Expression c1 = ExpressionFactory.createLiteral(ColumnType.INT, 7);
    Expression cmp0 = ExpressionFactory.createBinaryOperator(OperatorType.GE, ColumnType.BOOLEAN, ref0, c0);
    Expression cmp1 = ExpressionFactory.createBinaryOperator(OperatorType.LT, ColumnType.BOOLEAN, ref1, c1);
    DataSet f = Interpreter.filter(source, ImmutableList.of(cmp0, cmp1));
    DataSetIterator it = f.getIterator();
    assertTrue("no data found in filter dataset", it.next());
    assertEquals(it.get(0), 5);
    assertEquals(it.get(1), 6.3);
    assertFalse("too much data found in filter dataset", it.next());
  }

  @Test
  public void testMapperDataSet() {
    DataSet source = generatetestDataSet();
    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression c0 = ExpressionFactory.createLiteral(ColumnType.DOUBLE, 3.14);
    Expression perimeter = ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.DOUBLE, ref1, c0);
    Expression squre = ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.INT, ref0, ref0);
    Expression area = ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.DOUBLE, squre, c0);
    DataSet m = Interpreter.map(source, ImmutableList.of(perimeter, area));
    DataSetIterator it = m.getIterator();
    assertTrue("first row not found in map dataset", it.next());
    assertEquals(13.188, (double) it.get(0), 0.0001);
    assertEquals(28.26, (double) it.get(1), 0.0001);
    assertTrue("second row not found in map dataset", it.next());
    assertEquals(19.782, (double) it.get(0), 0.0001);
    assertEquals(78.5, (double) it.get(1), 0.0001);
    assertTrue("third row not found in map dataset", it.next());
    assertEquals(26.376, (double) it.get(0), 0.0001);
    assertEquals(153.86, (double) it.get(1), 0.0001);
    assertFalse("too much row in map dataset", it.next());
  }

  ProtoDataSet generateLeft() {
    final Schema schema = Schema.newBuilder().add("A", ColumnType.INT, Modifier.PUBLIC)
        .add("B", ColumnType.DOUBLE, Modifier.PUBLIC)
        .add("C", ColumnType.STRING, Modifier.PUBLIC).build();
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    ArrayRow.Builder builder = ArrayRow.newBuilder(3);
    builder.reset();
    builder.set(0, 3);
    builder.set(1, 4.2);
    builder.set(2, "Alice");
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 5);
    builder.set(1, 6.3);
    builder.set(2, "Bob");
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 7);
    builder.set(1, 8.4);
    builder.set(2, "Tom");
    dBuilder.addRow(builder.build());
    builder.reset();
    return dBuilder.build();
  }

  ProtoDataSet generateRight() {
    final Schema schema = Schema.newBuilder().add("A", ColumnType.INT, Modifier.PUBLIC)
        .add("B", ColumnType.DOUBLE, Modifier.PUBLIC)
        .add("C", ColumnType.STRING, Modifier.PUBLIC).build();
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    ArrayRow.Builder builder = ArrayRow.newBuilder(3);
    builder.reset();
    builder.set(0, 5);
    builder.set(1, 3.1);
    builder.set(2, "Snow");
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 2);
    builder.set(1, 6.3);
    builder.set(2, "Alice");
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 9);
    builder.set(1, 8.5);
    builder.set(2, "Bob");
    dBuilder.addRow(builder.build());
    builder.reset();
    return dBuilder.build();
  }

  /**
   * Left:
   * 3 | 4.2 | Alice
   * 5 | 6.3 | Bob
   * 7 | 8.4 | Tom
   * 
   * Right:
   * 5 | 3.1 | Snow
   * 2 | 6.3 | Alice
   * 9 | 8.5 | Bob
   */
  @Test
  public void testEqualJoinDataSet() {
    DataSet s0 = generateLeft();
    DataSet s1 = generateRight();
    JoinCondition condition = JoinCondition.newBuilder().setType(JoinType.INNER).addLeftKey(2).addRightKey(2).setModifier(Modifier.PUBLIC).build();
    DataSet res = Interpreter.join(s0, s1, condition);
    DataSetIterator it = res.getIterator();
    assertTrue(it.next());
    assertEquals(3, it.get(0));
    assertEquals(4.2, (double) it.get(1), 0.001);
    assertEquals("Alice", it.get(2));
    assertEquals(2, it.get(3));
    assertEquals(6.3, (double) it.get(4), 0.001);
    assertEquals("Alice", it.get(5));
    assertTrue(it.next());
    assertEquals(5, it.get(0));
    assertEquals(6.3, (double) it.get(1), 0.001);
    assertEquals("Bob", it.get(2));
    assertEquals(9, it.get(3));
    assertEquals(8.5, (double) it.get(4), 0.001);
    assertEquals("Bob", it.get(5));
    assertFalse(it.next());

  }

  @Test
  public void testThetaJoinDataSet() {
    DataSet s0 = generateLeft();
    DataSet s1 = generateRight();
    Expression leftRef = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression rightRef = ExpressionFactory.createInputRef(4, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression cmp = ExpressionFactory.createBinaryOperator(OperatorType.LT, ColumnType.BOOLEAN, leftRef, rightRef);
    JoinCondition condition = JoinCondition.newBuilder().setType(JoinType.INNER).setModifier(Modifier.PUBLIC).setCondition(cmp).build();
    DataSet res = Interpreter.join(s0, s1, condition);
    DataSetIterator it = res.getIterator();
    assertTrue(it.next());
    assertEquals(3, it.get(0));
    assertEquals(4.2, (double) it.get(1), 0.001);
    assertEquals("Alice", it.get(2));
    assertEquals(2, it.get(3));
    assertEquals(6.3, (double) it.get(4), 0.001);
    assertEquals("Alice", it.get(5));
    int count = 1;
    while (it.next()) {
      count++;
    }
    assertEquals(4, count);
  }

  @Test
  public void testcombineJoinDataSet() {
    DataSet s0 = generateLeft();
    DataSet s1 = generateRight();
    Expression leftRef = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression rightRef = ExpressionFactory.createInputRef(3, ColumnType.INT, Modifier.PUBLIC);
    Expression cmp = ExpressionFactory.createBinaryOperator(OperatorType.GT, ColumnType.BOOLEAN, leftRef, rightRef);
    JoinCondition condition = JoinCondition.newBuilder().setType(JoinType.INNER).setModifier(Modifier.PUBLIC).addLeftKey(2).addRightKey(2).setCondition(cmp).build();
    DataSet res = Interpreter.join(s0, s1, condition);
    DataSetIterator it = res.getIterator();
    assertTrue(it.next());
    assertEquals(3, it.get(0));
    assertEquals(4.2, (double) it.get(1), 0.001);
    assertEquals("Alice", it.get(2));
    assertEquals(2, it.get(3));
    assertEquals(6.3, (double) it.get(4), 0.001);
    assertEquals("Alice", it.get(5));
    assertFalse(it.next());
  }
}
