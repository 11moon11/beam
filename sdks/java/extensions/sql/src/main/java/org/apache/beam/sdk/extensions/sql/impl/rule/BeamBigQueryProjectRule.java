package org.apache.beam.sdk.extensions.sql.impl.rule;

import com.google.common.collect.ImmutableList;
import com.sun.istack.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.IntFunction;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BigQueryIOSourceRel;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

public class BeamBigQueryProjectRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final BeamBigQueryProjectRule INSTANCE =
      new BeamBigQueryProjectRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  public BeamBigQueryProjectRule(RelBuilderFactory relBuilderFactory) {
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

    Map<Integer, Integer> rexInputRefMapping = new HashMap<>();

    BigQueryIOSourceRel bigQueryIOSourceRel = constructNewIO(calc, ioSourceRel, rexInputRefMapping);
    //TODO: remove `calc.getProgram().getOutputRowType().getFieldCount() > 1`
    if (bigQueryIOSourceRel == null || calc.getProgram().getOutputRowType().getFieldCount() > 1) {
      return;
    }

    RelNode nCalc = reconstructCalc(calc, bigQueryIOSourceRel, call.builder(), rexInputRefMapping);

    //Calc result = constructNewCalc(calc, bigQueryIOSourceRel);
    call.getPlanner().setImportance(calc, 0.0);
    call.transformTo(nCalc);
  }

  /**
   * Generate new RowType for BigQueryIO
   * @param calc Calc
   * @param selectedFields List used by BigQuery for project
   * @return RowType to replace current BigQuery output and Calc input
   */
  private RelDataType newRowTypeForBigQuery(Calc calc, List<String> selectedFields, Map<Integer, Integer> rexInputRefMapping) {
    RexProgram program = calc.getProgram();
    //TODO: replace with FieldAccessDescriptor
    Set<String> requiredFields = new LinkedHashSet<>();
    RelDataTypeFactory.Builder relDataTypeBuilder = calc.getCluster().getTypeFactory().builder();

    final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter = program.split();
    // Find all input refs used by projects
    for (RexNode project : projectFilter.left) {
      findUtilizedInputRefs(program, project, requiredFields, relDataTypeBuilder, rexInputRefMapping);
    }
    // Find all input refs used by filters
    // TODO: uncomment
    /*for (RexNode filter : projectFilter.right) {
      findUtilizedInputRefs(program, filter, requiredFields, relDataTypeBuilder, rexInputRefMapping);
    }*/

    selectedFields.addAll(requiredFields);
    return relDataTypeBuilder.build();
  }

  /**
   * Given a RexNode find all RexInputRefs it or it's children use
   * @param program Calc program to search through
   * @param startNode Node to start at
   * @param requiredFields Used to keep track of input fields used by the node and it's children
   * @param relDataTypeBuilder Builder for new RelDataType containing only used input fields
   * @param inputMappings Records mappings of input field indexes in a new RelDataType
   */
  private void findUtilizedInputRefs(RexProgram program, RexNode startNode, Set<String> requiredFields, RelDataTypeFactory.Builder relDataTypeBuilder, Map<Integer, Integer> inputMappings) {
    Queue<RexNode> prerequisites = new ArrayDeque<>();
    prerequisites.add(startNode);

    // Assuming there are no cyclic nodes, traverse dependency tree until all RexInputRefs are found
    while (!prerequisites.isEmpty()) {
      RexNode node = prerequisites.poll();

      if (node instanceof RexCall) { // Composite expression, example: "=($t11, $t12)"
        RexCall compositeNode = (RexCall) node;

        // Expression from example above contains 2 operands: $t11, $t12
        prerequisites.addAll(compositeNode.getOperands());
      } else if (node instanceof RexInputRef) { // Input reference
        // Find a field in an inputRowType for the input reference
        int inputFieldIndex = ((RexInputRef) node).getIndex();
        RelDataTypeField field = program.getInputRowType().getFieldList().get(inputFieldIndex);

        // If we have not seen it before - add it to the list
        if (!requiredFields.contains(field.getName())) {
          inputMappings.put(inputFieldIndex, relDataTypeBuilder.getFieldCount());
          relDataTypeBuilder.add(field.getName(), field.getType());
          requiredFields.add(field.getName());
        }
      }
    }
  }

  /**
   * Construct an optimized BigQueryIO (with Project / Filter operations pushed-down)
   * @param calc Calc following BeamIOSourceRel
   * @param ioSourceRel IO sink, which will be transformed
   */
  @Nullable private BigQueryIOSourceRel constructNewIO(Calc calc, BeamIOSourceRel ioSourceRel, Map<Integer, Integer> rexInputRefMapping) {
    final BeamSqlTable table = ioSourceRel.getBeamSqlTable();
    // If IOSource does not support filter/project push-down - no work to be done
    if(!(table instanceof BigQueryTable)) {
      return null;
    }

    List<String> selectedFields = new ArrayList<>();
    RelDataType calcInput = newRowTypeForBigQuery(calc, selectedFields, rexInputRefMapping);
    // If no fields are being used - no work to be done
    if (selectedFields.size() == 0) {
      return null;
    }

    BigQueryTable bigQueryTable = (BigQueryTable) table;

    BigQueryIOSourceRel bigQueryIOSourceRel = new BigQueryIOSourceRel(ioSourceRel.getCluster(),
        ioSourceRel.getTable(),
        bigQueryTable,
        ioSourceRel.getPipelineOptions(),
        ioSourceRel.getCalciteTable(),
        selectedFields);

    bigQueryIOSourceRel.setRowType(calcInput);

    return bigQueryIOSourceRel;
  }

  private RelNode reconstructCalc(Calc currentCalc, BigQueryIOSourceRel bigQueryIOSourceRel, RelBuilder relBuilder, Map<Integer, Integer> rexInputRefMapping) {
    RexProgram program = currentCalc.getProgram();
    RelDataType newInputs = bigQueryIOSourceRel.getRowType();

    relBuilder.push(bigQueryIOSourceRel);

    List<RexNode> newProjects = new ArrayList<>();
    List<RexNode> newFilter = new ArrayList<>();
    final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter = program.split();

    // TODO: Attempt to move filter to IO (push-down)
    SqlNode condition = null;
    if (program.getCondition() != null) {
      condition = convertRelToSql(program, program.getExprList().get(program.getCondition().getIndex()));
    }
    if (condition == null) {
      // Rebuild all filters to use new input refs
      for (RexNode filter : projectFilter.right) {
        newFilter.add(reconstructRexNode(filter, newInputs, rexInputRefMapping));
      }
      relBuilder.filter(newFilter);
    } else {
      // TODO: if filter push-down is successfully, re-analyze what RexInputsRefs are needed for projects
    }

    // Rebuild all projects to use new input refs
    for (RexNode project : projectFilter.left) {
      newProjects.add(reconstructRexNode(project, newInputs, rexInputRefMapping));
    }
    relBuilder.project(newProjects, currentCalc.getRowType().getFieldNames());

    return relBuilder.build();
  }

  private RexNode reconstructRexNode(RexNode node, RelDataType newInputs, Map<Integer, Integer> rexInputRefMapping) {
    if (node instanceof RexInputRef) {
      int oldInputIndex = ((RexInputRef) node).getIndex();
      int newInputIndex = rexInputRefMapping.get(oldInputIndex);

      // Create a new input reference pointing to a new input field
      return new RexInputRef(newInputIndex, newInputs.getFieldList().get(newInputIndex).getType());
    } else if (node instanceof RexCall) { // Composite expression, example: "=($t11, $t12)"
      RexCall compositeNode = (RexCall) node;
      List<RexNode> newOperands = new ArrayList<>();

      for (RexNode operand : compositeNode.getOperands()) {
        newOperands.add(reconstructRexNode(operand, newInputs, rexInputRefMapping));
      }

      return compositeNode.clone(compositeNode.getType(), newOperands);
    }

    // If node is a Literal - return it as is
    return node;
  }

  private SqlNode convertRelToSql(RexProgram program, RexNode rex) {
    IntFunction<SqlNode> field = i ->  new SqlIdentifier(program.getInputRowType().getFieldList().get(i).getName(), SqlParserPos.ZERO);
    // TODO: Move this to Calcite BigQuery Dialect
    SqlImplementor.SimpleContext context = new SqlImplementor.SimpleContext(BigQuerySqlDialectWithTypeTranslation.DEFAULT, field); // SqlDialect.DatabaseProduct.CALCITE.getDialect()
    SqlNode sqlNode = context.toSql(program, rex);

    String sql = sqlNode.toSqlString(BigQuerySqlDialectWithTypeTranslation.DEFAULT).getSql();

    System.out.println("SQL condition looks something like this: " + sql);
    return sqlNode;
  }
}
