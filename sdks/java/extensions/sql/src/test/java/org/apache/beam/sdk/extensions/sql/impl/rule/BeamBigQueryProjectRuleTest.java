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
package org.apache.beam.sdk.extensions.sql.impl.rule;

import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.DOUBLE;
import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BYTE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT16;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;

import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BeamBigQueryProjectRuleTest {
  private static final Schema SOURCE_SCHEMA =
      Schema.builder()
          .addNullableField("c_bigint", INT64)
          .addNullableField("c_tinyint", BYTE)
          .addNullableField("c_smallint", INT16)
          .addNullableField("c_integer", INT32)
          .addNullableField("c_float", FLOAT)
          .addNullableField("c_double", DOUBLE)
          .addNullableField("c_boolean", BOOLEAN)
          .addNullableField("c_timestamp", CalciteUtils.TIMESTAMP)
          .addNullableField("c_varchar", STRING)
          .addNullableField("c_char", STRING)
          .addNullableField("c_arr", FieldType.array(STRING))
          .build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TestBigQuery bigQuery = TestBigQuery.create(SOURCE_SCHEMA);

  @Test
  public void testBeamBigQueryProjectRule() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQuery.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    String sqlQuery = "SELECT c_varchar FROM TEST";
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(sqlQuery));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));
  }
}
