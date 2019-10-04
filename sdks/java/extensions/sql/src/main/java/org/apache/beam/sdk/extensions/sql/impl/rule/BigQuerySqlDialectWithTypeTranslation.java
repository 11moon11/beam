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

import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.config.NullCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserPos;

public class BigQuerySqlDialectWithTypeTranslation extends BigQuerySqlDialect {
  public static final SqlDialect DEFAULT =
      new BigQuerySqlDialectWithTypeTranslation(
          EMPTY_CONTEXT
              .withDatabaseProduct(SqlDialect.DatabaseProduct.BIG_QUERY)
              .withNullCollation(NullCollation.LOW));

  public BigQuerySqlDialectWithTypeTranslation(SqlDialect.Context context) {
    super(context);
  }

  @Override
  public SqlNode getCastSpec(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case VARCHAR:
        // BigQuery doesn't have a VARCHAR type, only STRING.
        return typeFromName(type, "STRING");
      case BOOLEAN:
        return typeFromName(type, "BOOL");
      case INTEGER:
      case BIGINT:
        return typeFromName(type, "INT64");
      case TINYINT:
      case SMALLINT:
        return typeFromName(type, "NUMERIC");
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
        return typeFromName(type, "FLOAT64");
      case CHAR:
        return typeFromName(type, "BYTE");

      default:
        return super.getCastSpec(type);
    }
  }

  private static SqlNode typeFromName(RelDataType type, String name) {
    return new SqlDataTypeSpec(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        type.getPrecision(),
        -1,
        null,
        null,
        SqlParserPos.ZERO);
  }
}
