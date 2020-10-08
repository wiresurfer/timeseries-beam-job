package org.atria.operations.output;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.atria.operations.utils.TableRefPartition;
import org.joda.time.Duration;

import java.util.Properties;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.FILE_LOADS;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.STREAMING_INSERTS;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

/**
 * Created by Shaishav Kumar for ${PROJECT_NAMe} on 16/04/19.
 * Copyright Atria Power and/or pinclick
 * contact shaishav.kumar@atriapower.com
 */
public class WriteBq {
    private static final int BATCH_INTERVAL = 5;
    private static final int NUM_SHARDS = 10;


    public static BigQueryIO.Write<TableRow> WriteToBigQueryBatched(String tablePrefix,
                                                                    TableSchema schema,
                                                                    Properties options
                                                                    ) {
        String writeMode = options.getProperty("bq-write-mode", "batch");
        String disposition = options.getProperty("bq-write-disp", "batch");
        String tablename =  options.getProperty("bq-project", "dataglen-prod-196710") +":" +
                            options.getProperty("bq-database","dgmaster") + "." +
                            tablePrefix;

        if(writeMode == "streaming")
        {
            return BigQueryIO.writeTableRows()
                    .to(tablename)
                    .withSchema(schema)
                    .withMethod(STREAMING_INSERTS)
                    .withWriteDisposition(WRITE_APPEND)
                    .withCreateDisposition(CREATE_IF_NEEDED);
        }
        return BigQueryIO.writeTableRows()
//                .to(TableRefPartition.perDay(
//                        options.getProperty("bq-project", "dataglen-prod-196710"), //getBigqueryproject(),
//                        options.getProperty("bq-database","dgmaster"),
//                        tablePrefix))
                .to(tablename)
                .withSchema(schema)
                .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of("gs://bqtempwrite"))
                .withMethod(FILE_LOADS)
                .withTriggeringFrequency(Duration.standardMinutes(BATCH_INTERVAL))
                .withNumFileShards(NUM_SHARDS)
                .withWriteDisposition(WRITE_APPEND)
                .withCreateDisposition(CREATE_IF_NEEDED);

    }


}
