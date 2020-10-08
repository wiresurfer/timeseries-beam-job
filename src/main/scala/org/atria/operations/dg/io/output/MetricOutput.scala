package org.atria.operations.dg.io.output

import java.util.Properties

import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.{FILE_LOADS, STREAMING_INSERTS}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.{WRITE_APPEND,WRITE_TRUNCATE}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, TableDestination}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.atria.operations.dg.models.MetricTypes.SPointStat
import org.atria.operations.dg.io.output
import org.atria.operations.output.WriteBq
import org.atria.operations.utils.TableRefPartition
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, Duration}

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2019-10-25.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */
object MetricOutput {

  class DayPartitionFunction() extends SerializableFunction[ValueInSingleWindow[TableRow], TableDestination] {
    override def apply(input: ValueInSingleWindow[TableRow]): TableDestination = {
      val partition = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.UTC)
                          .print(input.getWindow.asInstanceOf[IntervalWindow].start())
      new TableDestination("project:dataset.partitioned$" + partition, "")
    }
  }


  def WriteToBigQueryBatched(tablePrefix: String, schema: TableSchema, options: Properties): BigQueryIO.Write[TableRow] = {
    val writeMode = options.getProperty("bq-write-mode", "batch")
    val writeDisposition = options.getProperty("bq-write-disposition", "WRITE_APPEND")

    var disposition =  WRITE_APPEND
    if(writeDisposition == "WRITE_TRUNCATE")
      disposition = WRITE_TRUNCATE

    if (writeMode == "streaming")
      BigQueryIO.writeTableRows.to(
        TableRefPartition.perDay(
          options.getProperty("bq-project", "dataglen-prod-196710"), //getBigqueryproject(),
          options.getProperty("bq-database", "dgmaster"),
          tablePrefix
        )
      )
        .withSchema(schema)
        .withMethod(STREAMING_INSERTS)
        .withWriteDisposition(disposition)
        .withCreateDisposition(CREATE_IF_NEEDED)
    else {
      val batchInterval = options.getProperty("bq-write-batch-interval", "5").toInt
      val numShards = options.getProperty("bq-write-num-shards", "5").toInt
      BigQueryIO.writeTableRows
        .to(
          TableRefPartition.perDay(
            options.getProperty("bq-project", "dataglen-prod-196710"),
            options.getProperty("bq-database", "dgmaster"),
            tablePrefix)
        )
        .withSchema(schema)
        .withMethod(FILE_LOADS)
        .withTriggeringFrequency(Duration.standardMinutes(batchInterval))
        .withNumFileShards(numShards)
        .withWriteDisposition(disposition)
        .withCreateDisposition(CREATE_IF_NEEDED)
    }
  }


  def BQOutputSink(outtag:String, instream:SCollection[SPointStat], bqprops:Properties = output.BQoutputProperties.DefaultBQProps("batch")) = {
    val outstream = instream.map(x => BigQueryType.toTableRow[SPointStat](x))
      .saveAsCustomOutput("bqout-"+outtag,
        WriteBq.WriteToBigQueryBatched(outtag,BigQueryType.schemaOf[SPointStat], bqprops)
      )
    outstream
  }

}
