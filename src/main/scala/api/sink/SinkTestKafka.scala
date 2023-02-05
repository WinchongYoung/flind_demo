package api.sink

import api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.util.Collector

/**
 * kafka sink
 */
object SinkTestKafka {
  private val alarmTag = new OutputTag[SensorReading]("alarm") {}
  private val normalTag = new OutputTag[SensorReading]("normal") {}

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 0.读取数据
    val inputPath = "src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      })

    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "test", new SimpleStringSchema()))

    env.execute("sink test")
  }
}