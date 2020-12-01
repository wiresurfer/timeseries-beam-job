package org.rapyuta.operations.rio

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.{SCollection, SideOutput}
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.rapyuta.operations.rio.models.MetricTypes.{RRMetricData, RRStringKV, SPointLastVal, SPointStat}
import org.rapyuta.operations.rio.io.input.Windowing
import org.rapyuta.operations.rio.transforms.SPointStatTransforms
import org.joda.time.{Duration, Instant}
import org.rapyuta.operations.rio.io.input.{Kafka, Windowing}
import org.rapyuta.operations.rio.models.MetricTypes.{SPointLastVal, SPointStat}
import org.rapyuta.operations.rio.transforms.SPointStatTransforms

import scala.language.higherKinds


/**
  * Created by Shaishav Kumar for  on 2019-10-23.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */

object DGAggregateStream {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
//    sc.optionsAs[StreamingOptions].setStreaming(true)

    val rawMetricStream = Kafka.inputKafkaStream(sc,"metric")
    val windowedDataStream =  rawMetricStream.withFixedWindows(
                                Duration.standardMinutes(1),
                                options = Windowing.lateFiringOptions
    )



    val lateData = SideOutput[(SPointStat,Long)]()
    val ontimeData = SideOutput[SPointStat]()
    val futureData = SideOutput[(SPointStat,Long)]()


    val (basestream:SCollection[SPointStat],sidestreams) = windowedDataStream
                      .withWindow[IntervalWindow]
                      .withSideOutputs(lateData,ontimeData,futureData)
                      .map{
                        case ((dgdata, w),ctx) => {
                          val key = RRStringKV(
                                      dgdata.tags.getOrElse("tenant_id","NA"),
                                      dgdata.tags.getOrElse("device_id","NA")
                                    )
                          val data_time = Utils.toDateTimeFromTS(dgdata.timestamp)
                          val tsinstant =  new Instant(data_time.toInstant.toEpochMilli)
                          val ps = SPointStatTransforms.newPointStat(key,w.start().toInstant, tsinstant,0.0)
                          val drift = (System.currentTimeMillis()/1000) - data_time.toEpochSecond()
                          val late = drift > 24*60*60l
                          val future = (0 - drift) > 24*60*60l
                          val ontime= !(late || future)
//                          if(late)
//                            ctx.output(lateData, (ps,drift))
//
//                          if(future)
//                            ctx.output(futureData, (ps,drift))
//
//                          if(ontime)
//                            ctx.output(ontimeData, ps)
                          ps
                        }

                      }



    val kvstream = basestream.keyBy(x => RRStringKV(x.source_key, x.stream_name))
//    sidestreams(lateData).map( x => {
//      print(x)
//    })
//    sidestreams(futureData).map( x => {
//      print(x)
//    })
    val lastValueStream = SideOutput[SPointLastVal]()
    val (pstatstream:SCollection[SPointStat], windowed_side_streams) = sidestreams(ontimeData)
                      .withFixedWindows(Duration.standardMinutes(15))
                      .keyBy(x => RRStringKV(x.source_key,x.stream_name))
                      .aggregateByKey(new SPointStat())( SPointStatTransforms.accumulate(_ , _), SPointStatTransforms.accumulate(_ , _))
                      .withWindow[IntervalWindow]
                      .withSideOutputs(lastValueStream)
                      .map {
                        case ( (x, w), ctx) => {
                          val (skv, ps) = x

                          val out_ps = SPointStatTransforms.copyPointStat(ps, w.start(),w.end())
                          ctx.output(lastValueStream, new SPointLastVal(ps.source_key, ps.stream_name,ps.last.toString,ps.ts_end))
                          val mil = System.currentTimeMillis()/1000
                          if(Math.floorDiv(mil,15) == 0l)
                            println(out_ps)
                          out_ps
                        }
                      }




//    val ftrstream = MetricOutput.BQOutputSink("freq_spoints",
//                                           SPointStatFilter.FilterOnlyFreqSpointStats(pstatstream),
//                                          BQoutputProperties.DefaultBQProps("streaming"))
//
//    val trstream = MetricOutput.BQOutputSink("spoints",pstatstream,
//                                          BQoutputProperties.DefaultBQProps("batch"))

    val result = sc.run()
    result.waitUntilDone()
  }
}
