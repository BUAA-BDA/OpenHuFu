package com.hufudb.openhufu.owner.implementor;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import com.hufudb.openhufu.data.function.AggregateFunction;
import com.hufudb.openhufu.expression.AggFuncType;
import com.hufudb.openhufu.owner.config.ImplementorConfig;
import com.hufudb.openhufu.owner.implementor.aggregate.OwnerAggregateFunction;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.rpc.Rpc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class OwnerImplementorFactory {
  public static Map<AggFuncType, String> aggFuncType2ClassName;

  static {
    aggFuncType2ClassName = new HashMap<>();
    aggFuncType2ClassName.put(AggFuncType.COUNT, ImplementorConfig.getImplementor(
        ImplementorConfig.Implementor.AGG_COUNT));
    aggFuncType2ClassName.put(AggFuncType.MAX, ImplementorConfig.getImplementor(
        ImplementorConfig.Implementor.AGG_MAX));
    aggFuncType2ClassName.put(AggFuncType.MIN, ImplementorConfig.getImplementor(
        ImplementorConfig.Implementor.AGG_MIN));
    aggFuncType2ClassName.put(AggFuncType.SUM, ImplementorConfig.getImplementor(
        ImplementorConfig.Implementor.AGG_SUM));
    aggFuncType2ClassName.put(AggFuncType.AVG, ImplementorConfig.getImplementor(
        ImplementorConfig.Implementor.AGG_AVG));
  }

  public static OwnerAggregateFunction getAggregationFunction(AggFuncType aggFuncType,
                                                              OpenHuFuPlan.Expression agg,
                                                              Rpc rpc,
                                                              ExecutorService threadPool,
                                                              OpenHuFuPlan.TaskInfo taskInfo) {
    String className = aggFuncType2ClassName.get(aggFuncType);
    try {
      Class clazz = Class.forName(className);
      Constructor constructor =
          clazz.getDeclaredConstructor(OwnerAggregateFunction.defaultConstructorClass());
      return (OwnerAggregateFunction) constructor.newInstance(agg, rpc, threadPool, taskInfo);
    } catch (ClassNotFoundException e) {
      throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CLASS_NOT_FOUND, className);
    } catch (NoSuchMethodException e) {
      throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CONSTRUCTOR_NOT_FOUND, className);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CREATE_FAILED, className);
    }

  }

  public static void main(String[] args) {
    AggregateFunction aggregateFunction =
        getAggregationFunction(AggFuncType.SUM, null, null, null, null);
    System.out.println("1");
  }
}
