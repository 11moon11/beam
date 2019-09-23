package org.apache.beam.sdk.extensions.sql.impl.rule;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.rel.BigTableIOSourceRel;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.*;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

public class BeamBigQueryProjectRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final BeamBigQueryProjectRule INSTANCE =
      new BeamBigQueryProjectRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  BeamBigQueryProjectRule(RelBuilderFactory relBuilderFactory) {
    super(operand(
            Calc.class,
            operand(BeamIOSourceRel.class, any())),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Calc calc = call.rel(0);
    final BeamIOSourceRel ioSourceRel = call.rel(1);

    final BeamSqlTable table = ioSourceRel.getBeamSqlTable();
    if(!(table instanceof BigQueryTable)) {
      return;
    }

    RexProgram program = calc.getProgram();
    List<Pair<RexLocalRef, String>> namedProjectList = program.getNamedProjects();
    if (namedProjectList.size() == 0) {
      return;
    }

    RelDataTypeFactory.Builder relDataTypeBuilder = calc.getCluster().getTypeFactory().builder();
    List<String> selectedFields = new ArrayList<>();
    for (Pair<RexLocalRef, String> namedProject : namedProjectList) {
      relDataTypeBuilder.add(namedProject.right, namedProject.left.getType()); // TODO: Check what other RowTypes are present in the filter that need to be passed as input to Calc
      selectedFields.add(namedProject.right);
    }

    if (selectedFields.size() == 0) {
      return;
    }

    BigQueryTable bigQueryTable = (BigQueryTable) table;
    bigQueryTable.setMethod(Method.DIRECT_READ);
    bigQueryTable.setSelectedFields(selectedFields);

    BigTableIOSourceRel bigTableIOSourceRel = new BigTableIOSourceRel(ioSourceRel.getCluster(),
        ioSourceRel.getTable(),
        bigQueryTable,
        ioSourceRel.getPipelineOptions(),
        ioSourceRel.getCalciteTable());

    RelDataType calcInput = relDataTypeBuilder.build();
    bigTableIOSourceRel.setRowType(calcInput);
    // Can quit here if Calc does nothing else
    call.transformTo(bigTableIOSourceRel);

    
  }
}
