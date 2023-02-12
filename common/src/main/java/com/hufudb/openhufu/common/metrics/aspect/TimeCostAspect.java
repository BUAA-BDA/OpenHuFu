package com.hufudb.openhufu.common.metrics.aspect;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import com.hufudb.openhufu.common.metrics.time.TimeCostManager;
import com.hufudb.openhufu.common.enums.TimeTerm;
import org.apache.commons.lang3.time.StopWatch;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class TimeCostAspect {

  private static final Logger
      LOG = LoggerFactory.getLogger(TimeCostAspect.class);

  @Pointcut("(execution(* *(..)) && @annotation(com.hufudb.openhufu.common.metrics.aspect.TimeCost))")
  private void operationCost() {
  }

  @Around("operationCost()")
  public Object handlingTimeAround(ProceedingJoinPoint joinPoint) {
    try {
      TimeTerm term = getAnnotation(joinPoint).term();
      LOG.info("Start watching {}.", term);
      StopWatch stopWatch = new StopWatch();
      stopWatch.start();
      Object proceed = joinPoint.proceed();
      stopWatch.stop();
      TimeCostManager.addTimeCostInfo(term,
          stopWatch.getTime(TimeUnit.MILLISECONDS));
      LOG.info("Stop watching {}. Time costs: {}ms.", term, stopWatch.getTime(TimeUnit.MILLISECONDS));
      return proceed;
    } catch (Throwable throwable) {
      LOG.error("Exception occur while calculating execution time");
      throw new OpenHuFuException(throwable, ErrorCode.INTERNAL_SERVER_ERROR);
    }
  }

  public TimeCost getAnnotation(ProceedingJoinPoint point) {
    Signature signature = point.getSignature();
    MethodSignature methodSignature = (MethodSignature) signature;
    Method method = methodSignature.getMethod();
    if (method != null) {
      return method.getAnnotation(TimeCost.class);
    }
    return null;
  }
}
