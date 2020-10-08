package org.atria.operations.dg

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import com.twitter.algebird.Moments
import org.atria.operations.vault.datatype.ScadaRecord
import org.joda.time.format.ISODateTimeFormat

//import scala.sys.process.processInternal.OutputStream

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2019-10-24.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */
object Utils {
//  def windowedWriteStream(prefix:String,w:IntervalWindow):OutputStream = {
//    val outputShard =
//      "%s-%s-%s".format(prefix, formatter.print(w.start()), formatter.print(w.end()))
//    val resourceId = FileSystems.matchNewResource(outputShard, false)
//    val out = Channels.newOutputStream(FileSystems.create(resourceId, MimeTypes.TEXT))
//    print(s"Window : ${outputShard}")
//    out
//
//  }
  def toMoment(x:Double):Moments = {
    Moments(x)
  }


  def toDateTimeFromTS(value:String):OffsetDateTime = {
    try {
      val ts_str = value + " Z"
      val ts = OffsetDateTime.parse(ts_str, DateTimeFormatter.ofPattern("dd/MM/yy-HH:mm:ss X"))
      ts
    }
    catch {
      case _ => return OffsetDateTime.now()
    }
  }

  private val formatter = ISODateTimeFormat.hourMinute

  //noinspection ScalaStyle
  def scadaRecordToString(s:ScadaRecord):String = {
    s"${s.getTag_name} / ${s.getRec_date_time.toString} => ${s.getTag_value.toString}"
  }

}
