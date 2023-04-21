package com.hufudb.openhufu.core.implementor.spatial.join;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.storage.utils.GeometryUtils;
import com.hufudb.openhufu.expression.ExpressionFactory;
import com.hufudb.openhufu.plan.BinaryPlan;
import com.hufudb.openhufu.plan.LeafPlan;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.proto.OpenHuFuData;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DistanceJoin {
  static final Logger LOG = LoggerFactory.getLogger(DistanceJoin.class);

  public static Plan generateDistanceQueryPlan(BinaryPlan binaryPlan, String point, int rightKey) {
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
    OpenHuFuPlan.Expression dwithin = ExpressionFactory.createScalarFunc(OpenHuFuData.ColumnType.BOOLEAN, "dwithin",
            ImmutableList.of(left, right, oldDwithin.getIn(2)));
    LOG.info("rewriting DistanceQueryPlan");
    List<OpenHuFuPlan.Expression> whereExps = ImmutableList.of(dwithin);
    leafPlan.setWhereExps(whereExps);
    leafPlan.setOrders(originalLeaf.getOrders());
    LOG.info(leafPlan.toString());
    return leafPlan;
  }
}
