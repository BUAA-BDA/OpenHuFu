package com.hufudb.onedb.owner.checker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.List;
import org.junit.Test;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.OperatorType;

public class CheckerTest {
  @Test
  public void testExpression() {
    List<Modifier> in = ImmutableList.of(Modifier.PROTECTED, Modifier.PUBLIC);
    Expression addModifierError = ExpressionFactory.createBinaryOperator(OperatorType.PLUS,
        ColumnType.INT, ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PUBLIC),
        ExpressionFactory.createLiteral(ColumnType.INT, 1));
    Expression addIndexError = ExpressionFactory.createBinaryOperator(OperatorType.PLUS,
        ColumnType.INT, ExpressionFactory.createInputRef(2, ColumnType.INT, Modifier.PUBLIC),
        ExpressionFactory.createLiteral(ColumnType.INT, 1));
    Expression addNormal = ExpressionFactory.createBinaryOperator(OperatorType.PLUS,
        ColumnType.INT, ExpressionFactory.createInputRef(0, ColumnType.INT, Modifier.PROTECTED),
        ExpressionFactory.createLiteral(ColumnType.INT, 1));
    Expression addHigher = ExpressionFactory.createBinaryOperator(OperatorType.PLUS,
        ColumnType.INT, ExpressionFactory.createInputRef(1, ColumnType.INT, Modifier.PROTECTED),
        ExpressionFactory.createLiteral(ColumnType.INT, 1));
    assertFalse(Checker.checkExpression(addModifierError, in));
    assertFalse(Checker.checkExpression(addIndexError, in));
    assertTrue(Checker.checkExpression(addNormal, in));
    assertTrue(Checker.checkExpression(addHigher, in));
  }
}
