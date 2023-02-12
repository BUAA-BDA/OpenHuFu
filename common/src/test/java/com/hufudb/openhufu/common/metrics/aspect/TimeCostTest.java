package com.hufudb.openhufu.common.metrics.aspect;

import static com.hufudb.openhufu.common.enums.TimeTerm.*;
import static org.junit.Assert.*;

import com.hufudb.openhufu.common.metrics.time.TimeCostManager;
import org.junit.After;
import org.junit.Test;

public class TimeCostTest {

  @Test
  @TimeCost(term = TOTAL_QUERY_TIME)
  public void timeAspect() throws InterruptedException {
    Thread.sleep(1000);
  }

  @After
  public void validate() {
    assertNotNull(TimeCostManager.getTimeCostInfo(TOTAL_QUERY_TIME));
  }
}