package com.hufudb.openhufu.core.implementor.spatial.knn;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.expression.AggFuncType;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.UnaryPlan;
import com.hufudb.openhufu.proto.OpenHuFuData;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BinarySearchKNN {
    static final Logger LOG = LoggerFactory.getLogger(BinarySearchKNN.class);

    public static Plan generateKNNRadiusQueryPlan(UnaryPlan originalPlan) {
        return null;
    }
    public static Plan generateDPRangeCountPlan(UnaryPlan originalPlan) {
        return null;
    }
    public static Plan generatePrivacyComparePlan(UnaryPlan originalPlan, double range) {
        LOG.info("rewriting PrivacyComparePlan");
        LeafPlan originalLeaf = (LeafPlan) originalPlan.getChildren().get(0);
        LeafPlan leafPlan = new LeafPlan();
        leafPlan.setTableName(originalLeaf.getTableName());
        leafPlan.setSelectExps(originalLeaf.getSelectExps());
        OpenHuFuPlan.Expression distance = originalLeaf.getSelectExps()
                .get(originalLeaf.getOrders().get(0).getRef());
        OpenHuFuPlan.Expression dwithin = ExpressionFactory.createScalarFunc(OpenHuFuData.ColumnType.BOOLEAN, "dwithin",
                ImmutableList.of(distance.getIn(0),
                        distance.getIn(1),
                        ExpressionFactory.createLiteral(OpenHuFuData.ColumnType.DOUBLE, range)));


        List<OpenHuFuPlan.Expression> whereExps = ImmutableList.of(dwithin);
        leafPlan.setWhereExps(whereExps);
        leafPlan.setAggExps(ImmutableList.of(ExpressionFactory.createAggFunc(OpenHuFuData.ColumnType.LONG,
                OpenHuFuData.Modifier.PROTECTED, AggFuncType.COUNT.getId(), ImmutableList.of())));
        leafPlan.setOrders(originalLeaf.getOrders());

        UnaryPlan unaryPlan = new UnaryPlan(leafPlan);
        unaryPlan.setSelectExps(ImmutableList.of(ExpressionFactory
                .createInputRef(0, OpenHuFuData.ColumnType.LONG, OpenHuFuData.Modifier.PROTECTED)));
        unaryPlan.setAggExps(ImmutableList.of(ExpressionFactory
                .createAggFunc(OpenHuFuData.ColumnType.LONG, OpenHuFuData.Modifier.PROTECTED, AggFuncType.SUM.getId(),
                        ImmutableList.of(ExpressionFactory
                .createInputRef(0, OpenHuFuData.ColumnType.LONG, OpenHuFuData.Modifier.PROTECTED)))));
        LOG.info(unaryPlan.toString());
        return unaryPlan;
    }
    public static Plan generateKNNCircleRangeQueryPlan(UnaryPlan originalPlan, double range) {
        LeafPlan originalLeaf = (LeafPlan) originalPlan.getChildren().get(0);
        LeafPlan leafPlan = new LeafPlan();
        leafPlan.setTableName(originalLeaf.getTableName());
        leafPlan.setSelectExps(originalLeaf.getSelectExps());
        OpenHuFuPlan.Expression distance = originalLeaf.getSelectExps()
                .get(originalLeaf.getOrders().get(0).getRef());
        OpenHuFuPlan.Expression dwithin = ExpressionFactory.createScalarFunc(OpenHuFuData.ColumnType.BOOLEAN, "dwithin",
                ImmutableList.of(distance.getIn(0),
                        distance.getIn(1),
                        ExpressionFactory.createLiteral(OpenHuFuData.ColumnType.DOUBLE, range)));
        LOG.info("rewriting KNNCircleRangeQueryPlan");
        List<OpenHuFuPlan.Expression> whereExps = ImmutableList.of(dwithin);
        leafPlan.setWhereExps(whereExps);
        leafPlan.setOrders(originalLeaf.getOrders());
        LOG.info(leafPlan.toString());
        return leafPlan;
    }
}
