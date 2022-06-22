package com.hufudb.onedb.interpreter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.lang.Float;
import java.lang.Double;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.ArrayRow;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.ProtoDataSet;
import com.hufudb.onedb.expression.AggFuncType;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import com.hufudb.onedb.proto.OneDBPlan.JoinType;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;
import org.junit.Test;

public class InterpreterTest {
  ProtoDataSet generateBoolDataSet() {
    final Schema schema = Schema.newBuilder().add("A", ColumnType.BOOLEAN, Modifier.PUBLIC)
        .add("B", ColumnType.BOOLEAN, Modifier.PUBLIC).build();
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    ArrayRow.Builder builder = ArrayRow.newBuilder(2);
    builder.reset();
    builder.set(0, true);
    builder.set(1, true);
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, true);
    builder.set(1, false);
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, false);
    builder.set(1, true);
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, false);
    builder.set(1, false);
    dBuilder.addRow(builder.build());
    builder.reset();
    return dBuilder.build();
  }

  ProtoDataSet generateTypeDataSet() {
    final Schema schema = Schema.newBuilder().add("A", ColumnType.INT, Modifier.PUBLIC)
        .add("B", ColumnType.DOUBLE, Modifier.PUBLIC).add("C", ColumnType.FLOAT, Modifier.PUBLIC)
        .add("D", ColumnType.LONG, Modifier.PUBLIC).build();
    ProtoDataSet.Builder dBuilder = ProtoDataSet.newBuilder(schema);
    ArrayRow.Builder builder = ArrayRow.newBuilder(4);
    builder.reset();
    builder.set(0, 3);
    builder.set(1, 4.2);
    builder.set(2, 16.16);
    builder.set(3, 114514);
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 5);
    builder.set(1, 6.3);
    builder.set(2, 12.12);
    builder.set(3, 415411);
    dBuilder.addRow(builder.build());
    builder.reset();
    builder.set(0, 7);
    builder.set(1, 8.4);
    builder.set(2, 18.18);
    builder.set(3, 373638);
    dBuilder.addRow(builder.build());
    builder.reset();
    return dBuilder.build();
  }

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
  public void testBoolDataSet() {
    DataSet source = generateBoolDataSet();
    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.BOOLEAN, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.BOOLEAN, Modifier.PUBLIC);

    Expression exp0 =
        ExpressionFactory.createBinaryOperator(OperatorType.AND, ColumnType.BOOLEAN, ref0, ref1);
    Expression exp1 =
        ExpressionFactory.createBinaryOperator(OperatorType.OR, ColumnType.BOOLEAN, ref0, ref1);
    Expression exp2 =
        ExpressionFactory.createUnaryOperator(OperatorType.NOT, ColumnType.BOOLEAN, ref0);

    DataSet m = Interpreter.map(source, ImmutableList.of(exp0, exp1, exp2));
    DataSetIterator it = m.getIterator();

    assertTrue("first row not found in map dataset", it.next());
    assertEquals(it.get(0), true);
    assertEquals(it.get(1), true);
    assertEquals(it.get(2), false);
    assertTrue("second row not found in map dataset", it.next());
    assertEquals(it.get(0), false);
    assertEquals(it.get(1), true);
    assertEquals(it.get(2), false);
    assertTrue("third row not found in map dataset", it.next());
    assertEquals(it.get(0), false);
    assertEquals(it.get(1), true);
    assertEquals(it.get(2), true);
    assertTrue("fourth row not found in map dataset", it.next());
    assertEquals(it.get(0), false);
    assertEquals(it.get(1), false);
    assertEquals(it.get(2), true);
    assertFalse("too much row in map dataset", it.next());
  }

  @Test
  public void testTypeDataSet() {
    DataSet source = generateTypeDataSet();
    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression ref2 = ExpressionFactory.createInputRef(2, ColumnType.FLOAT, Modifier.PUBLIC);
    Expression ref3 = ExpressionFactory.createInputRef(3, ColumnType.LONG, Modifier.PUBLIC);
    Expression c0 = ExpressionFactory.createLiteral(ColumnType.INT, 5);
    Expression c1 = ExpressionFactory.createLiteral(ColumnType.INT, 7);
    Expression c2 = ExpressionFactory.createLiteral(ColumnType.INT, 7);
    Expression c3 = ExpressionFactory.createLiteral(ColumnType.LONG, 415411);
    Expression cmp0 =
        ExpressionFactory.createBinaryOperator(OperatorType.LE, ColumnType.BOOLEAN, ref0, c0);
    Expression cmp1 =
        ExpressionFactory.createBinaryOperator(OperatorType.LT, ColumnType.BOOLEAN, ref1, c1);
    Expression cmp2 =
        ExpressionFactory.createBinaryOperator(OperatorType.NE, ColumnType.BOOLEAN, ref2, c2);
    Expression cmp3 =
        ExpressionFactory.createBinaryOperator(OperatorType.EQ, ColumnType.BOOLEAN, ref3, c3);
    DataSet f = Interpreter.filter(source, ImmutableList.of(cmp0, cmp1, cmp2, cmp3));
    DataSetIterator it = f.getIterator();
    assertTrue("no data found in filter dataset", it.next());
    assertEquals(it.get(0), 5);
    assertEquals(it.get(1), (double) 6.3);
    assertEquals(it.get(2), (float) 12.12);
    assertEquals(it.get(3), (long) 415411);
    assertFalse("too much data found in filter dataset", it.next());

    DataSet fEmpty = Interpreter.filter(source, new ArrayList<Expression>());
    DataSetIterator itEmpty = fEmpty.getIterator();
    assertTrue("no data found in filter dataset", itEmpty.next());
    assertEquals(itEmpty.get(0), 3);
    assertEquals(itEmpty.get(1), 4.2);
    assertEquals(itEmpty.get(2), (float) 16.16);
    assertEquals(itEmpty.get(3), (long) 114514);
    itEmpty.next();
    assertEquals(itEmpty.get(0), 5);
    assertEquals(itEmpty.get(1), 6.3);
    assertEquals(itEmpty.get(2), (float) 12.12);
    assertEquals(itEmpty.get(3), (long) 415411);
    itEmpty.next();
    assertEquals(itEmpty.get(0), 7);
    assertEquals(itEmpty.get(1), 8.4);
    assertEquals(itEmpty.get(2), (float) 18.18);
    assertEquals(itEmpty.get(3), (long) 373638);
    assertFalse("too much data found in filter dataset", itEmpty.next());
  }

  @Test
  public void testFilterCaseDataSet() {
    DataSet source = generatetestDataSet();
    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression c0 = ExpressionFactory.createLiteral(ColumnType.INT, 5);
    Expression c1 = ExpressionFactory.createLiteral(ColumnType.INT, 7);
    Expression cmp0 =
        ExpressionFactory.createBinaryOperator(OperatorType.GE, ColumnType.BOOLEAN, ref0, c0);
    Expression cmp1 =
        ExpressionFactory.createBinaryOperator(OperatorType.LT, ColumnType.BOOLEAN, ref1, c1);

    Expression case0 = ExpressionFactory.createMultiOperator(OperatorType.CASE, ColumnType.BOOLEAN,
        ImmutableList.of(cmp0, cmp1));

    DataSet f = Interpreter.filter(source, ImmutableList.of(cmp0, cmp1, case0));
    DataSetIterator it = f.getIterator();
    assertTrue("no data found in filter dataset", it.next());
    assertEquals(it.get(0), 5);
    assertEquals(it.get(1), 6.3);
    assertFalse("too much data found in filter dataset", it.next());

    DataSet fEmpty = Interpreter.filter(source, new ArrayList<Expression>());
    DataSetIterator itEmpty = fEmpty.getIterator();
    assertTrue("no data found in filter dataset", itEmpty.next());
    assertEquals(itEmpty.get(0), 3);
    assertEquals(itEmpty.get(1), 4.2);
    itEmpty.next();
    assertEquals(itEmpty.get(0), 5);
    assertEquals(itEmpty.get(1), 6.3);
    itEmpty.next();
    assertEquals(itEmpty.get(0), 7);
    assertEquals(itEmpty.get(1), 8.4);
    assertFalse("too much data found in filter dataset", itEmpty.next());
  }

  @Test
  public void testFilterDataSet() {
    DataSet source = generatetestDataSet();
    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression c0 = ExpressionFactory.createLiteral(ColumnType.INT, 5);
    Expression c1 = ExpressionFactory.createLiteral(ColumnType.INT, 7);
    Expression cmp0 =
        ExpressionFactory.createBinaryOperator(OperatorType.GE, ColumnType.BOOLEAN, ref0, c0);
    Expression cmp1 =
        ExpressionFactory.createBinaryOperator(OperatorType.LT, ColumnType.BOOLEAN, ref1, c1);

    DataSet f = Interpreter.filter(source, ImmutableList.of(cmp0, cmp1));
    DataSetIterator it = f.getIterator();
    assertTrue("no data found in filter dataset", it.next());
    assertEquals(it.get(0), 5);
    assertEquals(it.get(1), 6.3);
    assertFalse("too much data found in filter dataset", it.next());

    DataSet fEmpty = Interpreter.filter(source, new ArrayList<Expression>());
    DataSetIterator itEmpty = fEmpty.getIterator();
    assertTrue("no data found in filter dataset", itEmpty.next());
    assertEquals(itEmpty.get(0), 3);
    assertEquals(itEmpty.get(1), 4.2);
    itEmpty.next();
    assertEquals(itEmpty.get(0), 5);
    assertEquals(itEmpty.get(1), 6.3);
    itEmpty.next();
    assertEquals(itEmpty.get(0), 7);
    assertEquals(itEmpty.get(1), 8.4);
    assertFalse("too much data found in filter dataset", itEmpty.next());
  }

  @Test
  public void testMapperDataSet() {
    DataSet source = generatetestDataSet();
    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression c0 = ExpressionFactory.createLiteral(ColumnType.DOUBLE, 3.14);
    Expression perimeter =
        ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.DOUBLE, ref1, c0);
    Expression squre =
        ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.INT, ref0, ref0);
    Expression area =
        ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.DOUBLE, squre, c0);
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

    DataSet mEmpty = Interpreter.map(source, new ArrayList<Expression>());
    DataSetIterator itEmpty = mEmpty.getIterator();
    assertTrue("no data found in filter dataset", itEmpty.next());
    assertEquals(itEmpty.get(0), 3);
    assertEquals(itEmpty.get(1), 4.2);
    itEmpty.next();
    assertEquals(itEmpty.get(0), 5);
    assertEquals(itEmpty.get(1), 6.3);
    itEmpty.next();
    assertEquals(itEmpty.get(0), 7);
    assertEquals(itEmpty.get(1), 8.4);
    assertFalse("too much data found in filter dataset", itEmpty.next());

    Expression notNull =
        ExpressionFactory.createUnaryOperator(OperatorType.AS, ColumnType.INT, ref0);
    notNull =
        ExpressionFactory.createUnaryOperator(OperatorType.IS_NOT_NULL, ColumnType.BOOLEAN, ref0);
    Expression isNull =
        ExpressionFactory.createUnaryOperator(OperatorType.IS_NULL, ColumnType.BOOLEAN, ref1);
    DataSet mNull = Interpreter.map(source, ImmutableList.of(notNull, isNull));
    DataSetIterator itNull = mNull.getIterator();

    assertTrue("no data found in filter dataset", itNull.next());
    assertEquals(itNull.get(0), true);
    assertEquals(itNull.get(1), false);
    itNull.next();
    assertEquals(itNull.get(0), true);
    assertEquals(itNull.get(1), false);
    itNull.next();
    assertEquals(itNull.get(0), true);
    assertEquals(itNull.get(1), false);
    assertFalse("too much data found in filter dataset", itNull.next());
  }

  @Test
  public void testOperatorDataSet() {
    DataSet source = generateTypeDataSet();

    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression ref2 = ExpressionFactory.createInputRef(2, ColumnType.FLOAT, Modifier.PUBLIC);
    Expression ref3 = ExpressionFactory.createInputRef(3, ColumnType.LONG, Modifier.PUBLIC);

    Expression c0 = ExpressionFactory.createLiteral(ColumnType.INT, 7);
    Expression c1 = ExpressionFactory.createLiteral(ColumnType.DOUBLE, 7.7);
    Expression c2 = ExpressionFactory.createLiteral(ColumnType.FLOAT, 7.7);
    Expression c3 = ExpressionFactory.createLiteral(ColumnType.LONG, 7);

    Expression exp0 =
        ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.INT, ref0, c0);
    exp0 = ExpressionFactory.createBinaryOperator(OperatorType.DIVIDE, ColumnType.INT, exp0, c0);
    exp0 = ExpressionFactory.createUnaryOperator(OperatorType.MINUS_PRE, ColumnType.INT, exp0);
    exp0 = ExpressionFactory.createUnaryOperator(OperatorType.MINUS_PRE, ColumnType.INT, exp0);
    exp0 = ExpressionFactory.createUnaryOperator(OperatorType.PLUS_PRE, ColumnType.INT, exp0);
    exp0 = ExpressionFactory.createBinaryOperator(OperatorType.PLUS, ColumnType.INT, exp0, c0);
    exp0 = ExpressionFactory.createBinaryOperator(OperatorType.MINUS, ColumnType.INT, exp0, c0);
    Expression tmp0 =
        ExpressionFactory.createBinaryOperator(OperatorType.MOD, ColumnType.INT, exp0, exp0);
    exp0 = ExpressionFactory.createBinaryOperator(OperatorType.PLUS, ColumnType.INT, exp0, tmp0);

    Expression exp1 =
        ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.DOUBLE, ref1, c1);
    exp1 = ExpressionFactory.createBinaryOperator(OperatorType.DIVIDE, ColumnType.DOUBLE, exp1, c1);
    exp1 = ExpressionFactory.createUnaryOperator(OperatorType.MINUS_PRE, ColumnType.DOUBLE, exp1);
    exp1 = ExpressionFactory.createUnaryOperator(OperatorType.MINUS_PRE, ColumnType.DOUBLE, exp1);
    exp1 = ExpressionFactory.createUnaryOperator(OperatorType.PLUS_PRE, ColumnType.DOUBLE, exp1);
    exp1 = ExpressionFactory.createBinaryOperator(OperatorType.PLUS, ColumnType.DOUBLE, exp1, c1);
    exp1 = ExpressionFactory.createBinaryOperator(OperatorType.MINUS, ColumnType.DOUBLE, exp1, c1);
    Expression tmp1 =
        ExpressionFactory.createBinaryOperator(OperatorType.MOD, ColumnType.DOUBLE, exp1, exp1);
    exp1 = ExpressionFactory.createBinaryOperator(OperatorType.PLUS, ColumnType.DOUBLE, exp1, tmp1);

    Expression exp2 =
        ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.FLOAT, ref2, c2);
    exp2 = ExpressionFactory.createBinaryOperator(OperatorType.DIVIDE, ColumnType.FLOAT, exp2, c2);
    exp2 = ExpressionFactory.createUnaryOperator(OperatorType.MINUS_PRE, ColumnType.FLOAT, exp2);
    exp2 = ExpressionFactory.createUnaryOperator(OperatorType.MINUS_PRE, ColumnType.FLOAT, exp2);
    exp2 = ExpressionFactory.createUnaryOperator(OperatorType.PLUS_PRE, ColumnType.FLOAT, exp2);
    exp2 = ExpressionFactory.createBinaryOperator(OperatorType.PLUS, ColumnType.FLOAT, exp2, c2);
    exp2 = ExpressionFactory.createBinaryOperator(OperatorType.MINUS, ColumnType.FLOAT, exp2, c2);
    Expression tmp2 =
        ExpressionFactory.createBinaryOperator(OperatorType.MOD, ColumnType.FLOAT, exp2, exp2);
    exp2 = ExpressionFactory.createBinaryOperator(OperatorType.PLUS, ColumnType.FLOAT, exp2, tmp2);

    Expression exp3 =
        ExpressionFactory.createBinaryOperator(OperatorType.TIMES, ColumnType.LONG, ref3, c3);
    exp3 = ExpressionFactory.createBinaryOperator(OperatorType.DIVIDE, ColumnType.LONG, exp3, c3);
    exp3 = ExpressionFactory.createUnaryOperator(OperatorType.MINUS_PRE, ColumnType.LONG, exp3);
    exp3 = ExpressionFactory.createUnaryOperator(OperatorType.MINUS_PRE, ColumnType.LONG, exp3);
    exp3 = ExpressionFactory.createUnaryOperator(OperatorType.PLUS_PRE, ColumnType.LONG, exp3);
    exp3 = ExpressionFactory.createBinaryOperator(OperatorType.PLUS, ColumnType.LONG, exp3, c3);
    exp3 = ExpressionFactory.createBinaryOperator(OperatorType.MINUS, ColumnType.LONG, exp3, c3);
    Expression tmp3 =
        ExpressionFactory.createBinaryOperator(OperatorType.MOD, ColumnType.LONG, exp3, exp3);
    exp3 = ExpressionFactory.createBinaryOperator(OperatorType.PLUS, ColumnType.LONG, exp3, tmp3);

    DataSet m = Interpreter.map(source, ImmutableList.of(exp0, exp1, exp2, exp3));
    DataSetIterator it = m.getIterator();

    assertTrue("first row not found in map dataset", it.next());
    assertEquals(it.get(0), (int) 3);
    assertEquals((double) it.get(1), (double) 4.2, 0.0001);
    assertEquals(((Float) it.get(2)).doubleValue(), (double) 16.16, 0.0001);
    assertEquals((long) it.get(3), (long) 114514);
    assertTrue("second row not found in map dataset", it.next());
    assertEquals(it.get(0), (int) 5);
    assertEquals((double) it.get(1), (double) 6.3, 0.0001);
    assertEquals(((Float) it.get(2)).doubleValue(), (double) 12.12, 0.0001);
    assertEquals((long) it.get(3), (long) 415411);
    assertTrue("third row not found in map dataset", it.next());
    assertEquals(it.get(0), (int) 7);
    assertEquals((double) it.get(1), (double) 8.4, 0.0001);
    assertEquals(((Float) it.get(2)).doubleValue(), (double) 18.18, 0.0001);
    assertEquals((long) it.get(3), (long) 373638);
    assertFalse("too much row in map dataset", it.next());
  }

  ProtoDataSet generateLeft() {
    final Schema schema = Schema.newBuilder().add("A", ColumnType.INT, Modifier.PUBLIC)
        .add("B", ColumnType.DOUBLE, Modifier.PUBLIC).add("C", ColumnType.STRING, Modifier.PUBLIC)
        .build();
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
        .add("B", ColumnType.DOUBLE, Modifier.PUBLIC).add("C", ColumnType.STRING, Modifier.PUBLIC)
        .build();
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
   * Left: 3 | 4.2 | Alice 5 | 6.3 | Bob 7 | 8.4 | Tom
   * 
   * Right: 5 | 3.1 | Snow 2 | 6.3 | Alice 9 | 8.5 | Bob
   */
  @Test
  public void testEqualJoinDataSet() {
    DataSet s0 = generateLeft();
    DataSet s1 = generateRight();
    JoinCondition condition = JoinCondition.newBuilder().setType(JoinType.INNER).addLeftKey(2)
        .addRightKey(2).setModifier(Modifier.PUBLIC).build();
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
    Expression cmp = ExpressionFactory.createBinaryOperator(OperatorType.LT, ColumnType.BOOLEAN,
        leftRef, rightRef);
    JoinCondition condition = JoinCondition.newBuilder().setType(JoinType.INNER)
        .setModifier(Modifier.PUBLIC).setCondition(cmp).build();
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
    Expression cmp = ExpressionFactory.createBinaryOperator(OperatorType.GT, ColumnType.BOOLEAN,
        leftRef, rightRef);
    JoinCondition condition = JoinCondition.newBuilder().setType(JoinType.INNER)
        .setModifier(Modifier.PUBLIC).addLeftKey(2).addRightKey(2).setCondition(cmp).build();
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

  ProtoDataSet generatetestAggregateSourceDataSet() {
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
    builder.set(0, 5);
    builder.set(1, 4.2);
    dBuilder.addRow(builder.build());
    return dBuilder.build();
  }

  @Test
  public void testAggregate() {
    DataSet dataset = generatetestAggregateSourceDataSet();
    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression agg0 = ExpressionFactory.createAggFunc(ColumnType.INT, Modifier.PUBLIC,
        AggFuncType.COUNT.getId(), ImmutableList.of(ref0));
    Expression agg1 = ExpressionFactory.createAggFunc(ColumnType.DOUBLE, Modifier.PUBLIC,
        AggFuncType.SUM.getId(), ImmutableList.of(ref1));
    Expression agg2 = ExpressionFactory.createAggFunc(ColumnType.INT, Modifier.PUBLIC,
        AggFuncType.AVG.getId(), ImmutableList.of(ref0));
    Expression agg3 = ExpressionFactory.createAggFunc(ColumnType.DOUBLE, Modifier.PUBLIC,
        AggFuncType.MIN.getId(), ImmutableList.of(ref1));
    Expression agg4 = ExpressionFactory.createAggFunc(ColumnType.INT, Modifier.PUBLIC,
        AggFuncType.MAX.getId(), ImmutableList.of(ref0));
    List<Expression> aggs = ImmutableList.of(agg0, agg1, agg2, agg3, agg4);
    DataSet aggDataSet = Interpreter.aggregate(dataset, ImmutableList.of(), aggs);
    DataSetIterator it = aggDataSet.getIterator();
    assertTrue(it.next());
    assertEquals(4, it.get(0));
    assertEquals(23.1, (double) it.get(1), 0.001);
    assertEquals(5, it.get(2));
    assertEquals(4.2, it.get(3));
    assertEquals(7, it.get(4));
    assertFalse(false);

    DataSet aggDataSet2 =
        Interpreter.aggregate(dataset, ImmutableList.of(), new ArrayList<Expression>());
    DataSetIterator it2 = aggDataSet2.getIterator();
  }

  @Test
  public void testDistinctAggregate() {
    DataSet dataset = generatetestAggregateSourceDataSet();
    Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression agg0 = ExpressionFactory.createAggFunc(ColumnType.INT, Modifier.PUBLIC,
        -AggFuncType.COUNT.getId(), ImmutableList.of(ref0));
    Expression agg1 = ExpressionFactory.createAggFunc(ColumnType.DOUBLE, Modifier.PUBLIC,
        -AggFuncType.SUM.getId(), ImmutableList.of(ref1));
    Expression agg2 = ExpressionFactory.createAggFunc(ColumnType.INT, Modifier.PUBLIC,
        -AggFuncType.AVG.getId(), ImmutableList.of(ref0));
    Expression agg3 = ExpressionFactory.createAggFunc(ColumnType.DOUBLE, Modifier.PUBLIC,
        -AggFuncType.MIN.getId(), ImmutableList.of(ref1));
    Expression agg4 = ExpressionFactory.createAggFunc(ColumnType.INT, Modifier.PUBLIC,
        -AggFuncType.MAX.getId(), ImmutableList.of(ref0));
    List<Expression> aggs = ImmutableList.of(agg0, agg1, agg2, agg3, agg4);
    DataSet aggDataSet = Interpreter.aggregate(dataset, ImmutableList.of(), aggs);
    DataSetIterator it = aggDataSet.getIterator();
    assertTrue(it.next());
    assertEquals(3, it.get(0));
    assertEquals(18.9, (double) it.get(1), 0.001);
    assertEquals(5, it.get(2));
    assertEquals(4.2, it.get(3));
    assertEquals(7, it.get(4));
    assertFalse(false);
  }

  @Test
  public void testGroupAggregate() {
    DataSet dataset = generatetestAggregateSourceDataSet();
    // Expression ref0 = ExpressionFactory.createInputRef(0, ColumnType.INT,
    // Modifier.PUBLIC);
    Expression ref1 = ExpressionFactory.createInputRef(1, ColumnType.DOUBLE, Modifier.PUBLIC);
    Expression agg1 = ExpressionFactory.createAggFunc(ColumnType.DOUBLE, Modifier.PUBLIC,
        AggFuncType.AVG.getId(), ImmutableList.of(ref1));
    List<Expression> aggs = ImmutableList.of(agg1);
    DataSet aggDataSet = Interpreter.aggregate(dataset, ImmutableList.of(0), aggs);
    DataSetIterator it = aggDataSet.getIterator();
    Set<Double> expect = ImmutableSet.of(4.2, 8.4, 5.25);
    int count = 0;
    while (it.next()) {
      assertTrue(expect.contains(it.get(0)));
      count++;
    }
    assertEquals(3, count);
  }
}
