package org.apache.beam.sdk.extensions.sql.impl.rule;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BigTableIOSourceRel;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.*;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
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
            BeamCalcRel.class,
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
    // Can quit here if Calc does nothing else (works as intended)
    //call.transformTo(bigTableIOSourceRel);

    // When calc does something, need to modify inputs of the program
    final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter = program.split();
    /*if (projectFilter.right.size() == 0) {
      call.transformTo(bigTableIOSourceRel);
      return;
    }*/

    List<RexNode> list = program.getExprList();
    SqlTypeFactoryImpl sqlTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexNode rexNode = new RexInputRef(0, sqlTypeFactory.createTypeWithNullability(sqlTypeFactory.createSqlType(SqlTypeName.VARCHAR), true));

    // TODO: Remove project from the program, since it is being moved it IO
    final Calc result = calc.copy(calc.getTraitSet(), bigTableIOSourceRel, RexProgram.create(calcInput, ImmutableList.of(rexNode), program.getCondition(), program.getOutputRowType(), calc.getCluster().getRexBuilder()));

    // Does not have good enough cost to be chosen.
    call.transformTo(result);
  }
}
