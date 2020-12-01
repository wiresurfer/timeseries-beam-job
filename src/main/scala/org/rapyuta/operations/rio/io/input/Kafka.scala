package org.rapyuta.operations.rio.io.input

import scala.collection.JavaConverters._
import java.util.Properties

import com.google.common.collect.ImmutableMap
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.rapyuta.operations.rio.models.MetricTypes.RRMetricData
import org.joda.time.Instant
import org.rapyuta.operations.rio.Utils
import org.rapyuta.operations.rio.models.MetricTypes
import org.rapyuta.operations.rio.models.MetricTypes.RRMetricData

/**
  * Created by Shaishav Kumar for ${PROJECT_NAMe} on 2019-10-24.
  * Copyright Atria Power and/or pinclick
  * contact shaishav.kumar@atriapower.com
  */
object Kafka {

  val KAFKA_HOST = scala.util.Properties.envOrElse("KAFKA_HOST", "my-cluster-kafka-bootstrap-kafka.az39.rapyuta.io:443" )
  def DefaultKafkaProps(groupId:String) = {
    val kafkaprops:Map[String,String] = Map(
      ("group.id", groupId),
      ("enable.auto.commit",   "true"),
      ("auto.commit.interval.ms", "5000"),
      (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL"),
      (SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "./infra/clickhouse/truststore.jks"),
      (SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password")

    )
    kafkaprops
  }

  def inputKafkaStream(sc:ScioContext, topic:String, groupid:String = "sensoringest"):SCollection[RRMetricData] = {
    val props = DefaultKafkaProps("consumer-group-1");
    val rawstream  = sc
                      .customInput("Metric",
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
                          .updateConsumerProperties(props.asJava.asInstanceOf[java.util.Map[String, AnyRef]])
                          .withTimestampFn2( x => {
                            val kv = x.getKV()
                            println(kv.getKey)
//                            println(kv.getValue)
                            try {
                              val value = MetricTypes.objectMapper.readValue[RRMetricData](kv.getValue)

                            }
                            catch {
                              case e: Exception => println(kv.getValue)
                            }
//                            29/10/19-13:30:10
                            val value = MetricTypes.objectMapper.readValue[RRMetricData](kv.getValue)
                            val ts = Utils.toDateTimeFromTS(value.timestamp)
                            Instant.ofEpochSecond(ts.toEpochSecond)
                          })
                      )
                      .map(x => {
                        //TODO split into error stream at parsing level
                          val kv = x.getKV()
                          val value = MetricTypes.objectMapper.readValue[RRMetricData](kv.getValue)
                          val ts = Utils.toDateTimeFromTS(value.timestamp).toEpochSecond
                          print("+")
                          value
                      })
    rawstream
  }



}
