package api.streamapi

import api.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * split过期，使用process方法将流分为多个不同tag的流，然后获取
 */
object StreamTestSplit {
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

    alarmStream.print()
    // normalStream.print()

    env.execute("split test")
  }
}