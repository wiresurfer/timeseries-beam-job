package org.rapyuta.operations.rio.io.output

import java.util.Properties

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2019-11-05.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */
object BQoutputProperties {

  def DefaultBQProps(mode:String="batch",disposition:String="WRITE_APPEND", batchInterval:Int=5, numShards:Int=5) = {
    val bqprops = new Properties()
    bqprops.put("bq-write-mode", "batch")
    bqprops.put("bq-write-disposition", disposition)
    bqprops.put("bq-project",   "dataglen-prod-196710")
    bqprops.put("bq-database", "dgmaster")
    if(mode.equals("batch"))
    {
      bqprops.put("bq-write-batch-interval", batchInterval.toString)
      bqprops.put("bq-write-num-shards", numShards.toString)
    }


    bqprops
  }
}
