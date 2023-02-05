package api.simple

import api.SensorReading
import org.apache.flink.streaming.api.scala._

object TransformTest2Reduce {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 0.读取数据
    val inputPath = "src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    // 1.先转换成样例类类型（简单转换操作）
    // reduce
    // 输出当前输入传感器最新温度数据 + 10, 时间戳是上一次reduce结果 + 1
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }).keyBy(_.id)
      // curr：最新输入数据  result：上次reduce结果
      .reduce((current, result) => {
        SensorReading(current.id, current.timestamp + 10, result.temperature + 1)
      })
    dataStream.print()
    env.execute("transform test")
  }
}