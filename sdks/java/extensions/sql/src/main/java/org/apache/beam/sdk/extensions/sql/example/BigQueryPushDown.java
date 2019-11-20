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
        "CREATE EXTERNAL TABLE HACKER_NEWS( \n"
            + "   title VARCHAR, \n"
            + "   url VARCHAR, \n"
            + "   text VARCHAR, \n"
            + "   dead BOOLEAN, \n"
            + "   `by` VARCHAR, \n"
            + "   score INTEGER, \n"
            + "   `time` INTEGER, \n"
            + "   `timestamp` TIMESTAMP, \n"
            + "   type VARCHAR, \n"
            + "   id INTEGER, \n"
            + "   parent INTEGER, \n"
            + "   descendants INTEGER, \n"
            + "   ranking INTEGER, \n"
            + "   deleted BOOLEAN \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION 'bigquery-public-data:hacker_news.full' \n"
            + "TBLPROPERTIES '{ method: \"DIRECT_READ\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String selectTableStatement = "SELECT `by` as author, SUM(score) as total_score FROM HACKER_NEWS where type='story' group by `by` order by total_score desc limit 10";
    BeamRelNode beamRelNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    //long start = System.currentTimeMillis();
    pipeline.run().waitUntilFinish();
    //long finish = System.currentTimeMillis();
    //System.out.println("Time taken to run the pipeline: " + (finish - start));
  }
}
