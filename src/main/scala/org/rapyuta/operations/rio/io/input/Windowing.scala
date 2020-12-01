package org.rapyuta.operations.rio.io.input

import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, AfterWatermark, Repeatedly}
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2020-10-07.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */
object Windowing {
  val lateFiringOptions = WindowOptions(
                          trigger =Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
//                                    .withEarlyFirings(
//                                        windowing.AfterProcessingTime.pastFirstElementInPane()
//                                              .plusDelayOf(Duration.standardMinutes(1))
//                                              .alignedTo(Duration.standardMinutes(1))
//                                    ),
//                                    .withLateFirings(AfterPane.elementCountAtLeast(1)),
                          accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
                          closingBehavior = ClosingBehavior.FIRE_IF_NON_EMPTY,
                          allowedLateness = Duration.standardHours(24),
  )
  val dataStoreRegulateEarlyFiringOptions = WindowOptions(
    trigger = AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(
                                              AfterProcessingTime.pastFirstElementInPane()
                                                  .plusDelayOf(Duration.standardMinutes(5))
                                                  .alignedTo(Duration.standardMinutes(5))
                                        ),
    //                                    .withLateFirings(AfterPane.elementCountAtLeast(1)),
    accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
    closingBehavior = ClosingBehavior.FIRE_IF_NON_EMPTY,
    allowedLateness = Duration.standardHours(24),
  )
}
