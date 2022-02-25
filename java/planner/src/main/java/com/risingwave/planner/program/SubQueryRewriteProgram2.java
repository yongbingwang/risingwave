package com.risingwave.planner.program;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.rules.physical.BatchRuleSets;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;

public class SubQueryRewriteProgram2 implements OptimizerProgram {
  private static final Logger LOG =
      org.slf4j.LoggerFactory.getLogger(SubQueryRewriteProgram2.class);

  @Override
  public RelNode optimize(RelNode root, ExecutionContext context) {
    LOG.debug("SubQueryRewriteProgram2");

    var planner = new HepPlanner(semiJoinProgram(), context);
    planner.setRoot(root);

    var ret = planner.findBestExp();

    return ret;
  }

  private static HepProgram semiJoinProgram() {
    var builder = HepProgram.builder().addMatchOrder(HepMatchOrder.BOTTOM_UP);
    BatchRuleSets.SEMI_JOIN_RULES.forEach(builder::addRuleInstance);

    return builder.build();
  }
}
