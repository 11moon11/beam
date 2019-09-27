package org.apache.beam.sdk.extensions.sql.impl.rule;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class BigQuerySqlDialectWithTypeTranslation extends BigQuerySqlDialect {
  public static final SqlDialect DEFAULT =
      new BigQuerySqlDialectWithTypeTranslation(
          EMPTY_CONTEXT
              .withDatabaseProduct(SqlDialect.DatabaseProduct.BIG_QUERY)
              .withNullCollation(NullCollation.LOW));

  public BigQuerySqlDialectWithTypeTranslation(SqlDialect.Context context) {
    super(context);
  }

  @Override public SqlNode getCastSpec(RelDataType type) {
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
    return new SqlDataTypeSpec(new SqlIdentifier(name, SqlParserPos.ZERO),
        type.getPrecision(), -1, null, null, SqlParserPos.ZERO);
  }

}
