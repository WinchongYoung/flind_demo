package api.simple

import api.SensorReading
import org.apache.flink.streaming.api.scala._

object TransformTestKeyBy {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 0.读取数据
    val inputPath = "src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    // 1.先转换成样例类类型（简单转换操作）
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }).keyBy(_.id)
      .max("temperature")
    dataStream.print()
    env.execute("transform test")
  }
}