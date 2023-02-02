package com.hufudb.openhufu.core.sql.rule;

import com.hufudb.openhufu.core.sql.rule.FQFilterRule.FQFilterRuleConfig;
import com.hufudb.openhufu.core.sql.rule.FQLimitRule.FQLimitRuleConfig;
import org.apache.calcite.plan.RelOptRule;

public class FQRules {
  public static final FQToEnumerableConverterRule TO_ENUMERABLE =
      FQToEnumerableConverterRule.DEFAULT_CONFIG.toRule(FQToEnumerableConverterRule.class);
  public static final FQFilterRule FILTER =
      FQFilterRuleConfig.DEFAULT.toRule();
  public static final FQProjectRule PROJECT =
      FQProjectRule.DEFAULT_CONFIG.toRule(FQProjectRule.class);
  public static final FQCalcRule CALC = FQCalcRule.DEFAULT_CONFIG.toRule(FQCalcRule.class);
  public static final FQAggregateRule AGGREGATE =
      FQAggregateRule.DEFAULT_CONFIG.toRule(FQAggregateRule.class);
  public static final FQSortRule SORT = FQSortRule.DEFAULT_CONFIG.toRule(FQSortRule.class);
  public static final FQLimitRule LIMIT = FQLimitRuleConfig.DEFAULT.toRule();
  public static final FQJoinRule JOIN = FQJoinRule.DEFAULT_CONFIG.toRule(FQJoinRule.class);
  public static final RelOptRule[] RULES = {FILTER, PROJECT, CALC, AGGREGATE, SORT, LIMIT, JOIN};

  private FQRules() {}
}
