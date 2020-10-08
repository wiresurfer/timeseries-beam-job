package org.atria.operations.dg.io.input

import java.util.Properties

import com.google.common.collect.ImmutableMap
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.kafka.common.serialization.StringDeserializer
import org.atria.operations.dg.Utils
import org.atria.operations.dg.models.MetricTypes
import org.atria.operations.dg.models.MetricTypes.RRMetricData
import org.joda.time.Instant

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2019-10-24.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */
object Kafka {

  val KAFKA_HOST = scala.util.Properties.envOrElse("KAFKA_HOST", "35.197.154.83:9092" )
  def DefaultKafkaProps(groupId:String) = {
    val kafkaprops = new Properties()
    kafkaprops.put("group.id", groupId)
    kafkaprops.put("enable.auto.commit",   "true")
    kafkaprops.put("auto.commit.interval.ms", "5000")
    kafkaprops
  }

  def inputKafkaStream(sc:ScioContext, topic:String, groupid:String = "sensoringest"):SCollection[RRMetricData] = {
    val rawstream  = sc
                      .customInput("LavanyaTopic",
                        KafkaIO.read[String, String]
                          //Required Config
                          .withTopic(topic)
                          //TODO   Read properties from KafkaProp:Properties?  Make it more flexible.
                          .withBootstrapServers(KAFKA_HOST)
                          //DEFINE group id and AUTO_COMMIT)
                          .updateConsumerProperties(ImmutableMap.of("group.id", groupid))
                          .updateConsumerProperties(ImmutableMap.of("enable.auto.commit", "false"))
                          .updateConsumerProperties(ImmutableMap.of("auto.commit.interval.ms", "5000"))
                          .commitOffsetsInFinalize()
                          .withValueDeserializer(classOf[StringDeserializer])
                          .withKeyDeserializer(classOf[StringDeserializer])

                          .withTimestampFn2( x => {
                            val kv = x.getKV()
                            val value = MetricTypes.objectMapper.readValue[RRMetricData](kv.getValue)
                            val ts = Utils.toDateTimeFromTS(value.timestamp_in_data)
//                            29/10/19-13:30:10
                            Instant.ofEpochSecond(ts.toEpochSecond)
                          })
                      )
                      .map(x => {
                        //TODO split into error stream at parsing level
                          val kv = x.getKV()
                          val value = MetricTypes.objectMapper.readValue[RRMetricData](kv.getValue)
                          val ts = Utils.toDateTimeFromTS(value.timestamp_in_data).toEpochSecond
                          print("+")
                          value
                      })
    rawstream
  }


}
