package tk.onedb.core.sql.rule;

import org.apache.calcite.plan.RelOptRule;

public class OneDBRules {
  private OneDBRules() {
  }

  public static final OneDBToEnumerableConverterRule TO_ENUMERABLE = OneDBToEnumerableConverterRule.DEFAULT_CONFIG
      .toRule(OneDBToEnumerableConverterRule.class);

  public static final OneDBFilterRule FILTER = OneDBFilterRule.Config.DEFAULT.toRule();

  public static final OneDBProjectRule PROJECT = OneDBProjectRule.DEFAULT_CONFIG.toRule(OneDBProjectRule.class);

  public static final OneDBCalcRule CALC = OneDBCalcRule.Config.DEFAULT.toRule();

  public static final OneDBAggregateRule AGGREGATE = OneDBAggregateRule.DEFAULT_CONFIG.toRule(OneDBAggregateRule.class);

  public static final RelOptRule[] RULES = { FILTER, PROJECT, CALC, AGGREGATE };
}
