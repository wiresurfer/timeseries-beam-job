package org.rapyuta.operations.rio.transforms

import com.spotify.scio.bigquery.TableRow
import org.rapyuta.operations.rio.models.MetricTypes.{RRStringKV, SPointStat}
import org.joda.time.Instant

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2019-11-05.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */
object SPointStatTransforms {
  def toTableRow(x:SPointStat):TableRow = {
    val y = new TableRow()
    y.set("source_key", x.source_key)
    y.set("source_key", x.source_key)
    y.set("source_key", x.source_key)
    y.set("source_key", x.source_key)
  }

  def newPointStat(sksn:RRStringKV, ts:Instant, ts_end:Instant, value:Double):SPointStat = {
    val count = 1
    val mean = value
    val sum = value
    val stddev = 0
    val min = value
    val max = value
    val first = value
    val last = value
    SPointStat(sksn.proj_id,sksn.dev_id, ts,ts_end, count,mean,stddev,sum,min,max,first,last)
  }

  def copyPointStat(in:SPointStat, ts:Instant=Instant.EPOCH, ts_end:Instant=Instant.EPOCH):SPointStat = {
    var out_ts = in.ts
    if(ts!= Instant.EPOCH)
      out_ts = ts

    var out_ts_end = in.ts_end
    if(ts_end != Instant.EPOCH)
      out_ts_end = ts_end
    SPointStat(in.source_key, in.stream_name, out_ts, out_ts_end,in.count, in.mean, in.stddev, in.sum, in.min, in.max, in.first, in.last)
  }

  def accumulate(a: SPointStat, b: SPointStat): SPointStat = {
    if (a.source_key == null && b.source_key == null) return null
    else if (a.source_key == null && (a.count == 0l)) return b
    else if (b.source_key == null && (b.count == 0l)) return a
    if (!(a.source_key == b.source_key)) return null
    if ((a.count == 0l) && (b.count == 0l)) return null

    val source_key = a.source_key
    val stream_name = a.stream_name
    val count = a.count + b.count
    val max = if (a.max >= b.max) a.max else b.max
    val min = if (a.min <= b.min) a.min else b.min
    val sum = a.sum + b.sum
    val mean = (a.sum + b.sum) / (a.count + b.count)
    val stddev = Math.sqrt(a.count * (Math.pow(a.stddev, 2) + Math.pow(a.mean - mean, 2)) + b.count * (Math.pow(b.stddev, 2) + Math.pow(b.mean - mean, 2))) / (a.count + b.count)

    var ts = a.ts
    if (a.ts == Instant.EPOCH) ts = b.ts
    if (b.ts == Instant.EPOCH) ts = a.ts
    ts = if (a.ts.isBefore(b.ts)) a.ts else b.ts

    val ts_end = if (a.ts_end.isAfter(b.ts_end)) a.ts_end else b.ts_end

    var first = if (a.ts.isBefore(b.ts)) a.first else b.first
    if(a.ts.isEqual(b.ts))
    {
        first = if (a.ts_end.isBefore(b.ts_end)) a.first else b.first
    }

    val last = if (a.ts_end.isAfter(b.ts_end)) a.last else b.last
    SPointStat(source_key,stream_name,ts,ts_end,count,mean,stddev,sum,min,max,first,last)
  }
}
