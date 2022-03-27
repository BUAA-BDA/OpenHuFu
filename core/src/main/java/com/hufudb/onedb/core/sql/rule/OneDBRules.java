package com.hufudb.onedb.core.sql.rule;

import org.apache.calcite.plan.RelOptRule;

public class OneDBRules {
  private OneDBRules() {
  }

  public static final OneDBToEnumerableConverterRule TO_ENUMERABLE = OneDBToEnumerableConverterRule.DEFAULT_CONFIG
      .toRule(OneDBToEnumerableConverterRule.class);

  public static final OneDBFilterRule FILTER = OneDBFilterRule.OneDBFilterRuleConfig.DEFAULT.toRule();

  public static final OneDBProjectRule PROJECT = OneDBProjectRule.DEFAULT_CONFIG.toRule(OneDBProjectRule.class);

  public static final OneDBCalcRule CALC = OneDBCalcRule.OneDBCalcRuleConfig.DEFAULT.toRule();

  public static final OneDBAggregateRule AGGREGATE = OneDBAggregateRule.DEFAULT_CONFIG.toRule(OneDBAggregateRule.class);

  public static final OneDBJoinRule JOIN = OneDBJoinRule.DEFAULT_CONFIG.toRule(OneDBJoinRule.class);

  public static final RelOptRule[] RULES = { FILTER, PROJECT, CALC, AGGREGATE, JOIN };
}
