package api.streamapi

import api.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 流的合并
 * 1、connect只能合并两条流
 * 2、两个流数据类型可以不一致
 */
object TransformTestConnect {
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
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 4. 多流转换操作
    // 4.1 分流，将传感器温度数据分成低温、高温两条流
    val splitStream = dataStream.process(new ProcessFunction[SensorReading, SensorReading] {
      override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
        if (value.temperature >= 35) {
          ctx.output(alarmTag, value)
        } else {
          ctx.output(normalTag, value)
        }
      }
    })

    val alarmStream = splitStream.getSideOutput(alarmTag)
    val normalStream = splitStream.getSideOutput(normalTag)

    val connectedStream = alarmStream.connect(normalStream)
    val coMapStream = connectedStream.map(warningData => {
      (warningData.id, "waring")
    },
      normalData => {
        (normalData.id, normalData.temperature, "normal")
      })

    coMapStream.print()

    env.execute("connect test")
  }
}