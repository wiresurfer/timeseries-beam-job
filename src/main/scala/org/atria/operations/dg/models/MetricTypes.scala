package org.atria.operations.dg.models

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.spotify.scio
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.Instant

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2019-10-24.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */




object MetricTypes {
  @scio.bigquery.BigQueryType.toTable
  case class RRStringKV(source_key:String, stream_name:String)

  @scio.bigquery.BigQueryType.toTable
  case class RRMetricData(timestamp_in_data:String, //
                          source_key:String, //
                          stream_name:String, //
                          plant_slug:String, //
                          client_slug:String, //
                          //                       device_type:String,  //
                          plant_id:String, //
                          raw_value:String, //
                          stream_value:String )//

  @scio.bigquery.BigQueryType.toTable
  case class ValidDataStore(
                               source_key:String,
                               stream_name:String,
                               timestamp_data:String,
                               insertion_time:String,
                               stream_value:String
                             )

  @scio.bigquery.BigQueryType.toTable
  case class SPointLastVal(
                            source_key:String,
                            stream_name:String,
                            last_value: String,
                            last_seen: Instant = Instant.EPOCH
                          )

  @scio.bigquery.BigQueryType.toTable
  case class SPointStat(source_key: String = null,
                        stream_name: String = null,
                        ts: Instant = Instant.EPOCH,
                        ts_end: Instant = Instant.EPOCH ,
                        count: Long = 0,
                        mean: Double = 0,
                        stddev: Double = 0.0,
                        sum: Double = 0.0,
                        min: Double = Double.MaxValue,
                        max: Double = Double.MinValue,
                        first: Double = Double.MinValue,
                        last: Double = Double.MaxValue)


  val RRMetricDataBQType = BigQueryType[RRMetricData]
  val SPointStatBQType = BigQueryType[SPointStat]


  case class StreamDrift(site:String, drift:Double)
  case class SourceDrift(source_key:String, drift:Double)


  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
  objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
  objectMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS,true)

}

