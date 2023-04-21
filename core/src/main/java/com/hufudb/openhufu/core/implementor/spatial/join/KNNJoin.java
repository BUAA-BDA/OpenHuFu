package com.hufudb.openhufu.core.implementor.spatial.join;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.storage.utils.GeometryUtils;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.plan.BinaryPlan;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.UnaryPlan;
import com.hufudb.openhufu.proto.OpenHuFuData;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KNNJoin {
  static final Logger LOG = LoggerFactory.getLogger(KNNJoin.class);

  public static Plan generateKNNQueryPlan(BinaryPlan binaryPlan, String point, int rightKey) {
    LOG.info(String.valueOf(rightKey));
    LeafPlan originalLeaf = (LeafPlan) binaryPlan.getChildren().get(1);
    LeafPlan leafPlan = new LeafPlan();
    leafPlan.setTableName(originalLeaf.getTableName());
    if (rightKey == -1) {
      leafPlan.setSelectExps(originalLeaf.getSelectExps());
    }
    else {
      List<OpenHuFuPlan.Expression> selects = new ArrayList<>();
      int i = 0;
      for (OpenHuFuPlan.Expression expression: originalLeaf.getSelectExps()) {
        if (i != rightKey) {
          selects.add(expression);
        }
        i++;
      }
      leafPlan.setSelectExps(selects);
    }
    OpenHuFuPlan.Expression oldDwithin = binaryPlan.getJoinCond().getCondition();
    OpenHuFuPlan.Expression left = ExpressionFactory.createLiteral(OpenHuFuData.ColumnType.GEOMETRY, GeometryUtils.fromString(point));
    OpenHuFuPlan.Expression right = ExpressionFactory.createInputRef(originalLeaf.getSelectExps().get(oldDwithin.getIn(1).getI32()
                    - binaryPlan.getChildren().get(0).getSelectExps().size()).getI32(), oldDwithin.getIn(1).getOutType(),
            oldDwithin.getIn(1).getModifier());
    OpenHuFuPlan.Expression dwithin = ExpressionFactory.createScalarFunc(OpenHuFuData.ColumnType.BOOLEAN, "knn",
            ImmutableList.of(left, right, oldDwithin.getIn(2)));
    LOG.info("rewriting DistanceQueryPlan");
    List<OpenHuFuPlan.Expression> whereExps = ImmutableList.of(dwithin);
    leafPlan.setWhereExps(whereExps);
    leafPlan.setOrders(originalLeaf.getOrders());
    LOG.info(leafPlan.toString());
    return convertKNN(leafPlan);
  }

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
