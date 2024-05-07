package com.hufudb.openhufu.owner.implementor;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import com.hufudb.openhufu.expression.AggFuncType;
import com.hufudb.openhufu.owner.config.ImplementorConfig;
import com.hufudb.openhufu.owner.config.ImplementorConfig.Implementor;
import com.hufudb.openhufu.owner.implementor.aggregate.OwnerAggregateFunction;
import com.hufudb.openhufu.owner.implementor.join.OwnerJoin;
import com.hufudb.openhufu.owner.implementor.union.OwnerUnion;
import com.hufudb.openhufu.proto.OpenHuFuPlan;
import com.hufudb.openhufu.rpc.Rpc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class OwnerImplementorFactory {

  public static Map<AggFuncType, String> aggFuncType2ClassName;
  public static String joinClassName;

  public static String unionClassName;

  static {
    aggFuncType2ClassName = new HashMap<>();
    aggFuncType2ClassName.put(AggFuncType.SUM, ImplementorConfig.getImplementorClassName(
        Implementor.AGG_SUM));
    aggFuncType2ClassName.put(AggFuncType.MAX, ImplementorConfig.getImplementorClassName(
        Implementor.AGG_MAX));
    aggFuncType2ClassName.put(AggFuncType.MIN, ImplementorConfig.getImplementorClassName(
        Implementor.AGG_MIN));
//    aggFuncType2ClassName.put(AggFuncType.COUNT, ImplementorConfig.getImplementorClassName(
//        Implementor.AGG_COUNT));
//    aggFuncType2ClassName.put(AggFuncType.AVG, ImplementorConfig.getImplementorClassName(
//        Implementor.AGG_AVG));
    joinClassName = ImplementorConfig.getImplementorClassName(Implementor.JOIN);
    unionClassName = ImplementorConfig.getImplementorClassName(Implementor.UNION);
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
      throw new OpenHuFuException(e , ErrorCode.IMPLEMENTOR_CONSTRUCTOR_NOT_FOUND, className);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CREATE_FAILED, className);
    }

  }

  public static OwnerJoin getJoin() {
    try {
      Class clazz = Class.forName(joinClassName);
      Constructor constructor =
          clazz.getDeclaredConstructor();
      return (OwnerJoin) constructor.newInstance();
    } catch(ClassNotFoundException e){
        throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CLASS_NOT_FOUND, joinClassName);
      } catch(NoSuchMethodException e){
        throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CONSTRUCTOR_NOT_FOUND, joinClassName);
      } catch(InstantiationException | IllegalAccessException | InvocationTargetException e){
        throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CREATE_FAILED, joinClassName);
      }
    }

  public static OwnerUnion getUnion() {
    try {
      Class clazz = Class.forName(unionClassName);
      Constructor constructor =
              clazz.getDeclaredConstructor();
      return (OwnerUnion) constructor.newInstance();
    } catch(ClassNotFoundException e){
      throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CLASS_NOT_FOUND, unionClassName);
    } catch(NoSuchMethodException e){
      throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CONSTRUCTOR_NOT_FOUND, unionClassName);
    } catch(InstantiationException | IllegalAccessException | InvocationTargetException e){
      throw new OpenHuFuException(e, ErrorCode.IMPLEMENTOR_CREATE_FAILED, unionClassName);
    }
  }

    public static void main (String[]args){
      OwnerJoin ownerJoin = getJoin();
    }
  }
