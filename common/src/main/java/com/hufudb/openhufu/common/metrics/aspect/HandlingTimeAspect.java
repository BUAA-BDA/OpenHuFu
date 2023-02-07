package com.hufudb.openhufu.common.metrics.aspect;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import com.hufudb.openhufu.common.metrics.time.TimeManager;
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
public class HandlingTimeAspect {

  private static final Logger
      LOG = LoggerFactory.getLogger(HandlingTimeAspect.class);

  @Pointcut("(execution(* *(..)) && @annotation(com.hufudb.openhufu.common.metrics.aspect.HandlingTime))")
  private void operationLog() {
  }

  @Around("operationLog()")
  public Object handlingTimeAround(ProceedingJoinPoint joinPoint) {
    try {
      StopWatch stopWatch = new StopWatch();
      stopWatch.start();
      Object proceed = joinPoint.proceed();
      stopWatch.stop();
      TimeManager.addTimeInfo(getAnnotation(joinPoint).name(),
          stopWatch.getTime(TimeUnit.MILLISECONDS));
      System.out.println(getAnnotation(joinPoint).name());
      System.out.println(stopWatch.getTime(TimeUnit.MILLISECONDS));
      return proceed;
    } catch (Throwable throwable) {
      LOG.error("Exception occur while calculating execution time");
      throw new OpenHuFuException(throwable, ErrorCode.INTERNAL_SERVER_ERROR);
    }
  }

  public HandlingTime getAnnotation(ProceedingJoinPoint point) {
    Signature signature = point.getSignature();
    MethodSignature methodSignature = (MethodSignature) signature;
    Method method = methodSignature.getMethod();
    if (method != null) {
      return method.getAnnotation(HandlingTime.class);
    }
    return null;
  }
}
