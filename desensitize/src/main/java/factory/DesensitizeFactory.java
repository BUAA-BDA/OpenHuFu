package factory;

import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.data.schema.utils.PojoMethod;
import com.hufudb.onedb.desensitize.ExpSensitivityConvert;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.proto.OneDBData.Sensitivity;
import com.hufudb.onedb.proto.OneDBData.Desensitize;
import com.hufudb.onedb.proto.OneDBData.Method;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import com.hufudb.onedb.proto.OneDBPlan.ExpSensitivity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DesensitizeFactory {
    private static Boolean desensitize = true;

    private static SchemaManager manager = null;

    public static Object implement(Object val, ColumnDesc columnDesc) {
        if (!desensitize) {
            return val;
        }
        Object rt = val;
        Desensitize desensitize = columnDesc.getDesensitize();
        Sensitivity sensitivity = desensitize.getSensitivity();
        Method method = desensitize.getMethod();
        PojoMethod pojoMethod = PojoMethod.fromColumnMethod(method);
        switch (sensitivity) {
            case PLAIN:
                break;
            case SENSITIVE:
                rt = pojoMethod.implement(val, columnDesc, method);
                break;
            case SECRET:
                rt = null;
                break;
        }
        return rt;
    }

    public static void checkDesensitization() {
        if (!desensitize) {
            return;
        }
        for (Map.Entry<String, TableSchema> entry : manager.desensitizationMap.entrySet()) {
            String tableName = entry.getKey();
            TableSchema actualTable = manager.actualTableSchemaMap.get(tableName);
            for (ColumnDesc columnDesc : entry.getValue().getSchema().getColumnDescs()) {
                String colName = columnDesc.getName();
                ColumnType localType = actualTable.getSchema().getType(actualTable.getColumnIndex(colName));
                ColumnType desensitizationType = columnDesc.getType();
                Desensitize desensitize = columnDesc.getDesensitize();
                PojoMethod pojoMethod = PojoMethod.fromColumnMethod(desensitize.getMethod());
                pojoMethod.check(localType, desensitizationType);
            }
        }
    }

    public static void checkSensitivity(Plan plan) {
        if (!desensitize) {
            return;
        }
        switch (plan.getPlanType()) {
            case LEAF:
                sensitivityExps(manager, plan);
                break;
            case UNARY:
                checkSensitivity(plan.getChildren().get(0));
                List<Expression> inputs = plan.getChildren().get(0).getOutExpressions();
                sensitivityExps(plan, inputs);
                break;
            case BINARY:
                checkSensitivity(plan.getChildren().get(0));
                checkSensitivity(plan.getChildren().get(1));
                List<Expression> leftInputs = plan.getChildren().get(0).getOutExpressions();
                List<Expression> rightInputs = plan.getChildren().get(1).getOutExpressions();
                List<Expression> allInputs = new ArrayList<>();
                allInputs.addAll(leftInputs);
                allInputs.addAll(rightInputs);
                sensitivityExps(plan, allInputs);
                checkJoinCond(plan.getJoinCond(), leftInputs, rightInputs);
                break;
            case EMPTY:
                break;
        }
    }

    public static void checkJoinCond(JoinCondition joinCondition, List<Expression> leftInputs, List<Expression> rightInputs) {
        for (int key : joinCondition.getLeftKeyList()) {
            Expression keyExp = leftInputs.get(key);
            if (keyExp.getSensitivity() != ExpSensitivity.NONE_SENSITIVE) {
                throw new RuntimeException(String.format("\nJoinCondition:%sLeft key is sensitive, can't do join", joinCondition));
            }
        }
        for (int key : joinCondition.getRightKeyList()) {
            Expression keyExp = rightInputs.get(key);
            if (keyExp.getSensitivity() != ExpSensitivity.NONE_SENSITIVE) {
                throw new RuntimeException(String.format("\nJoinCondition:%sRight key is sensitive, can't do join", joinCondition));
            }
        }
    }

    public static Expression sensitivityExp(Expression exp, SchemaManager manager, Plan plan, List<Expression> allInputs) {
        Expression rt = ExpressionFactory.addSensitivity(exp, ExpSensitivity.NONE_SENSITIVE);
        switch (exp.getOpType()) {
            case REF:
                if (allInputs == null) {
                    TableSchema desensitizeTable = manager.getDesensitizationMap().get(manager.getActualTableName(plan.getTableName()));
                    String refName = manager.getPublishedSchema(plan.getTableName()).getName(exp.getI32());
                    Desensitize desensitize = desensitizeTable.getDesensitize(refName);
                    if (desensitize.getSensitivity() != Sensitivity.PLAIN) {
                        rt = ExpressionFactory.addSensitivity(exp, ExpSensitivity.SINGLE_SENSITIVE);
                    }
                } else {
                    ExpSensitivity expSensitivity = allInputs.get(exp.getI32()).getSensitivity();
                    if (expSensitivity != ExpSensitivity.NONE_SENSITIVE) {
                        rt = ExpressionFactory.addSensitivity(exp, ExpSensitivity.SINGLE_SENSITIVE);
                    }
                }
                break;
            case LITERAL:
                break;
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
            case MOD:
            case GT:
            case GE:
            case LT:
            case LE:
            case EQ:
            case NE:
            case AND:
            case OR:
            case LIKE:
                assert exp.getInList().size() == 2;
                Map<ExpSensitivity, Integer> map = new HashMap<>();
                List<Expression> ins = new ArrayList<>();
                for (Expression e : exp.getInList()) {
                    ExpSensitivity tmp = sensitivityExp(e, manager, plan, allInputs).getSensitivity();
                    ins.add(exp);
                    map.merge(tmp, 1, Integer::sum);
                }
                rt = ExpressionFactory.addSensitivity(exp, ExpSensitivityConvert.convertBinary(map), ins);
                break;

            case AS:
            case NOT:
            case PLUS_PRE:
            case MINUS_PRE:
            case IS_NULL:
            case IS_NOT_NULL:
                assert exp.getInList().size() == 1;
                rt =  sensitivityExp(exp.getIn(0), manager, plan, allInputs);
                break;

            case AGG_FUNC:
                if (exp.getI32() != 1) {
                    Expression tmp = sensitivityExp(exp.getIn(0), manager, plan, allInputs);
                    rt = ExpressionFactory.addSensitivity(exp, ExpSensitivityConvert.convertAggFunctions(tmp.getSensitivity(), exp.getI32()));
                    break;
                }
        }
        if (rt.getSensitivity() == ExpSensitivity.ERROR) {
            throw new RuntimeException(String.format("%s  Can't desensitize", exp));
        }
        return rt;
    }

    public static void sensitivityExps(SchemaManager manager, Plan plan) {
        ArrayList<Expression> expList = new ArrayList<>();
        for (Expression exp : plan.getSelectExps()) {
            Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, null).getSensitivity());
            expList.add(expression);
        }
        plan.setSelectExps(expList);
        expList = new ArrayList<>();
        for (Expression exp : plan.getWhereExps()) {
            Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, null).getSensitivity());
            expList.add(expression);
        }
        plan.setWhereExps(expList);
        expList = new ArrayList<>();
        for (Expression exp : plan.getAggExps()) {
            Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, null).getSensitivity());
            expList.add(expression);
        }
        plan.setAggExps(expList);
    }


    public static void sensitivityExps(Plan plan, List<Expression> allInputs) {
        ArrayList<Expression> expList = new ArrayList<>();
        for (Expression exp : plan.getSelectExps()) {
            Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, allInputs).getSensitivity());
            expList.add(expression);
        }
        plan.setSelectExps(expList);
        expList = new ArrayList<>();
        for (Expression exp : plan.getWhereExps()) {
            Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, allInputs).getSensitivity());
            expList.add(expression);
        }
        plan.setWhereExps(expList);
        expList = new ArrayList<>();
        for (Expression exp : plan.getAggExps()) {
            Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, allInputs).getSensitivity());
            expList.add(expression);
        }
        plan.setAggExps(expList);
    }

    public static Boolean getDesensitize() {
        return desensitize;
    }

    public static void setDesensitize(Boolean d) {
        desensitize = d;
    }

    public static SchemaManager getManager() {
        return manager;
    }

    public static void setSchemaManager(SchemaManager manager) {
        DesensitizeFactory.manager = manager;
    }
}
