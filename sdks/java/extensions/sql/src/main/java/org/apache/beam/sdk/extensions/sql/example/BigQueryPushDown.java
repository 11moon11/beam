package org.apache.beam.sdk.extensions.sql.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class BigQueryPushDown {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE GIT_CONTENTS( \n"
            + "   id VARCHAR, \n"
            + "   size INTEGER, \n"
            + "   content VARCHAR, \n"
            + "   `binary` BOOLEAN, \n"
            + "   copies INTEGER \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION 'bigquery-public-data:github_repos.contents' \n"
            + "TBLPROPERTIES '{ method: \"DIRECT_READ\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String selectTableStatement = "SELECT id, size FROM GIT_CONTENTS where `binary`=false";
    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    //long start = System.currentTimeMillis();
    pipeline.run().waitUntilFinish();
    //long finish = System.currentTimeMillis();
    //System.out.println("Time taken to run the pipeline: " + (finish - start));
  }
}
