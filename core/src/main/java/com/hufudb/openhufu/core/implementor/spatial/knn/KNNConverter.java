package com.hufudb.openhufu.core.implementor.spatial.knn;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.UnaryPlan;
import com.hufudb.openhufu.proto.OpenHuFuData;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KNNConverter {
  static final Logger LOG = LoggerFactory.getLogger(KNNConverter.class);
  public static UnaryPlan convertKNN(LeafPlan plan) {
    LOG.info("converting KNN");
    LeafPlan leafPlan = new LeafPlan();
    leafPlan.setTableName(plan.getTableName());
    List<OpenHuFuPlan.Expression> selects1 = new ArrayList<>(plan.getSelectExps());
    OpenHuFuPlan.Expression knn = plan.getWhereExps().get(0);
    OpenHuFuPlan.Expression distance = ExpressionFactory
            .createScalarFunc(OpenHuFuData.ColumnType.DOUBLE, "distance",
            ImmutableList.of(knn.getIn(0), knn.getIn(1)));
    selects1.add(distance);
    leafPlan.setSelectExps(selects1);
    OpenHuFuPlan.Collation order1 = OpenHuFuPlan.Collation.newBuilder().setRef(plan.getSelectExps().size())
            .setDirection(OpenHuFuPlan.Direction.ASC).build();
    leafPlan.setOrders(ImmutableList.of(order1));
    leafPlan.setFetch((int) knn.getIn(2).getF64());

    UnaryPlan unaryPlan = new UnaryPlan(leafPlan);
    List<OpenHuFuPlan.Expression> selects2 = new ArrayList<>(plan.getSelectExps());
    selects2.add(ExpressionFactory.createInputRef(order1.getRef(), OpenHuFuData.ColumnType.DOUBLE, OpenHuFuData.Modifier.PROTECTED));
    unaryPlan.setSelectExps(selects2);
    OpenHuFuPlan.Collation order2 = OpenHuFuPlan.Collation.newBuilder().setRef(plan.getSelectExps().size())
            .setDirection(OpenHuFuPlan.Direction.ASC).build();
    unaryPlan.setOrders(ImmutableList.of(order2));
    unaryPlan.setFetch(leafPlan.getFetch());
    LOG.info(unaryPlan.toString());
    return unaryPlan;
  }
}
