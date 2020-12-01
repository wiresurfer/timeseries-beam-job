package org.rapyuta.operations.rio.transforms

import com.spotify.scio.values.SCollection
import org.rapyuta.operations.rio.models.MetricTypes.SPointStat

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2019-11-05.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */
object SPointStatFilter {
  val STREAMS =
    Array(
      ("PlantMeta", Array("IRRADIATION", "AMBIENT_TEMPERATURE", "MODULE_TEMPERATURE", "WINDSPEED", "HUMIDITY", "RAIN")),
      ("Inverter" , Array("SOLAR_STATUS","ACTIVE_POWER", "CURRENT", "AC_VOLTAGE", "APPARENT_POWER", "DAILY_YIELD","TOTAL_YIELD", "DC_CURRENT", "DC_POWER", "DC_VOLTAGE")),
      ("Meter"    , Array("WATT_TOTAL", "Wh_DELIVERED", "Wh_RECEIVED")),
      ("AJB"      , Array("VOLTAGE", "CURRENT", "POWER")),
    )
  val FREQ_STREAM_NAMES = STREAMS.flatMap(x => x._2)


  def FilterOnlyFreqSpointStats(instream:SCollection[SPointStat]) = {
    instream.filter( x => FREQ_STREAM_NAMES contains x.stream_name)
  }
}
