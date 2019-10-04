/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl.rel;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteTable;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.TableScan;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;

/** BeamRelNode to replace a {@code TableScan} node. */
public class BigQueryIOSourceRel extends TableScan implements BeamRelNode {
  public static final double CONSTANT_WINDOW_SIZE = 10d;
  private final BigQueryTable beamTable;
  private final BeamCalciteTable calciteTable;
  private final Map<String, String> pipelineOptions;
  private List<String> selectedFields;

  public BigQueryIOSourceRel(
      RelOptCluster cluster,
      RelOptTable table,
      BigQueryTable beamTable,
      Map<String, String> pipelineOptions,
      BeamCalciteTable calciteTable) {
    super(cluster, cluster.traitSetOf(BeamLogicalConvention.INSTANCE), table);
    this.beamTable = beamTable;
    this.calciteTable = calciteTable;
    this.pipelineOptions = pipelineOptions;
    this.selectedFields = null;
  }

  public BigQueryIOSourceRel(
      RelOptCluster cluster,
      RelOptTable table,
      BigQueryTable beamTable,
      Map<String, String> pipelineOptions,
      BeamCalciteTable calciteTable,
      List<String> selectedFields) {
    this(cluster, table, beamTable, pipelineOptions, calciteTable);
    this.selectedFields = selectedFields;
  }

  public void setRowType(RelDataType rowType) {
    this.rowType = rowType;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    /*BeamTableStatistics rowCountStatistics = calciteTable.getStatistic();
    if (beamTable.isBounded() == PCollection.IsBounded.BOUNDED) {
      return rowCountStatistics.getRowCount();
    } else {
      return rowCountStatistics.getRate();
    }*/
    return 0.0000000000001;
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    BeamTableStatistics rowCountStatistics = calciteTable.getStatistic();
    double window =
        (beamTable.isBounded() == PCollection.IsBounded.BOUNDED)
            ? rowCountStatistics.getRowCount()
            : CONSTANT_WINDOW_SIZE;
    return NodeStats.create(rowCountStatistics.getRowCount(), rowCountStatistics.getRate(), window);
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return beamTable.isBounded();
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> input) {
      checkArgument(
          input.size() == 0,
          "Should not have received input for %s: %s",
          BigQueryIOSourceRel.class.getSimpleName(),
          input);

      // TODO: Not use hard-coded condition
      BigQueryIOSourceRel.this.selectedFields = ImmutableList.of("c_integer");
      return beamTable.buildIOReader(
          input.getPipeline().begin(),
          BigQueryIOSourceRel.this.selectedFields,
          "CAST(c_tinyint AS INT64) = 127 AND CAST(c_smallint AS INT64) = 32767 AND (c_integer = 2147483647 OR CAST(c_float AS FLOAT64) = 1.0 AND c_double = 1.0 AND c_integer - 10 = 2147483637)");
      /*
      if (BigQueryIOSourceRel.this.selectedFields == null || BigQueryIOSourceRel.this.selectedFields.isEmpty()) {
        return beamTable.buildIOReader(input.getPipeline().begin());
      } else {
        return beamTable.buildIOReader(input.getPipeline().begin(), BigQueryIOSourceRel.this.selectedFields);
      }*/
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // We should technically avoid this function. This happens if we are in JDBC path or the
    // costFactory is not set correctly.
    double rowCount = this.estimateRowCount(mq);
    return planner.getCostFactory().makeCost(rowCount, rowCount, rowCount);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    NodeStats estimates = BeamSqlRelUtils.getNodeStats(this, mq);
    return BeamCostModel.FACTORY.makeCost(estimates.getRowCount(), estimates.getRate());
  }

  public BigQueryTable getBeamSqlTable() {
    return beamTable;
  }

  @Override
  public Map<String, String> getPipelineOptions() {
    return pipelineOptions;
  }
}
