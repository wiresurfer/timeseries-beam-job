package org.rapyuta.operations.rio

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.rapyuta.operations.rio.models.MetricTypes.RRStringKV
import org.rapyuta.operations.rio.models.MetricTypes.RRStringKV

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2019-10-25.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */
object Transforms {


  type DoFnT = DoFn[ KV[String, RRStringKV], KV[String, (Int, Int)]]
//  class DriftBySource extends DoFnT {
//    @StateId("count") private val count = StateSpecs.value[JInt]()
//
//    @ProcessElement
//    def processElement(
//                        context: DoFnT#ProcessContext,
//                        @StateId("count") count: ValueState[JInt]
//    ): Unit = {
//        val c = count.read()
//        count.write(c + 1)
//        val kv = context.element()
//        val source = context.element().getKey()
//
//        context.output(KV.of(source, (kv.getValue.stream_name, c)))
//    }
//  }

}
