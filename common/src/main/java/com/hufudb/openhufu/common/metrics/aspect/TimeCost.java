package com.hufudb.openhufu.common.metrics.aspect;

import com.hufudb.openhufu.common.enums.TimeTerm;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface TimeCost {
  TimeTerm term();

}
