package com.hufudb.openhufu.owner.implementor.aggregate;

import com.hufudb.openhufu.data.function.AggregateFunction;
import com.hufudb.openhufu.data.storage.Row;
import com.hufudb.openhufu.proto.OpenHuFuData;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.rpc.Rpc;
import java.util.concurrent.ExecutorService;

public abstract class OwnerAggregateFunction implements AggregateFunction<Row, Comparable> {

  protected final int inputRef;
  protected final OpenHuFuData.ColumnType type;
  protected final OpenHuFuPlan.TaskInfo taskInfo;

  public OwnerAggregateFunction(int inputRef, OpenHuFuData.ColumnType type, OpenHuFuPlan.TaskInfo taskInfo) {
    this.inputRef = inputRef;
    this.type = type;
    this.taskInfo = taskInfo;
  }
  /*
  * the parameters of constructor for each aggregate function class
   */
  public static Class[] defaultConstructorClass() {
    return new Class[] {OpenHuFuPlan.Expression.class, Rpc.class, ExecutorService.class,
        OpenHuFuPlan.TaskInfo.class};
  }

}
