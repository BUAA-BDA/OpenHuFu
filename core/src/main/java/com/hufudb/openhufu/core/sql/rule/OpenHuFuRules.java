package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rule.OpenHuFuLimitRule.OpenHuFuLimitRuleConfig;
import org.apache.calcite.plan.RelOptRule;

public class OpenHuFuRules {
  public static final OpenHuFuToEnumerableConverterRule TO_ENUMERABLE =
      OpenHuFuToEnumerableConverterRule.DEFAULT_CONFIG.toRule(OpenHuFuToEnumerableConverterRule.class);
  public static final OpenHuFuFilterRule FILTER =
      OpenHuFuFilterRule.OpenHuFuFilterRuleConfig.DEFAULT.toRule();
  public static final OpenHuFuProjectRule PROJECT =
      OpenHuFuProjectRule.DEFAULT_CONFIG.toRule(OpenHuFuProjectRule.class);
  public static final OpenHuFuCalcRule CALC = OpenHuFuCalcRule.DEFAULT_CONFIG.toRule(OpenHuFuCalcRule.class);
  public static final OpenHuFuAggregateRule AGGREGATE =
      OpenHuFuAggregateRule.DEFAULT_CONFIG.toRule(OpenHuFuAggregateRule.class);
  public static final OpenHuFuSortRule SORT = OpenHuFuSortRule.DEFAULT_CONFIG.toRule(OpenHuFuSortRule.class);
  public static final OpenHuFuLimitRule LIMIT = OpenHuFuLimitRuleConfig.DEFAULT.toRule();
  public static final OpenHuFuJoinRule JOIN = OpenHuFuJoinRule.DEFAULT_CONFIG.toRule(OpenHuFuJoinRule.class);
  public static final RelOptRule[] RULES = {FILTER, PROJECT, CALC, AGGREGATE, SORT, LIMIT, JOIN};

  private OpenHuFuRules() {}
}
