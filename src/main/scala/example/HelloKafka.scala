package example

import monix.execution.Scheduler.Implicits.global
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable, KafkaProducer, KafkaProducerConfig}
import monix.kafka.config.AutoOffsetReset

import scala.concurrent.Await
import scala.concurrent.duration._
import monix.execution.Scheduler


object HelloKafka extends App {

  lazy val io = Scheduler.io("monix-kafka-tests")

  // Init
  val topicName = "monix-kafka-tests"

  val producerCfg = KafkaProducerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:9092"),
    clientId = "monix-kafka-1-0-producer-test"
  )

  val consumerCfg = KafkaConsumerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:9092"),
    groupId = "kafka-tests",
    clientId = "monix-kafka-1-0-consumer-test",
    autoOffsetReset = AutoOffsetReset.Earliest
  )


  val count = 10000

  val producer = KafkaProducer[String,String](producerCfg, io)
  val consumer = KafkaConsumerObservable[String,String](consumerCfg, List(topicName)).executeOn(io)
  try {
    // Publishing one message
    val send = producer.send(topicName, "The quick brown fox jumps over the lazy dog ")
    Await.result(send.runAsync, 30.seconds)

    val first = consumer.take(1).map(_.value()).firstL
    val result = Await.result(first.runAsync, 30.seconds)
    println(s"RESULT: $result")
  }
  finally {
    Await.result(producer.close().runAsync, Duration.Inf)
  }
}

