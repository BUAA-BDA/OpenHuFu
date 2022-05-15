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
}
