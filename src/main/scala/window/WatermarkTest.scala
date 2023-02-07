package window

import api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *
 */
object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(50)

    // 读取数据
    val inputStream = env.socketTextStream("localhost", 7777)
    inputStream.print()

    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })

    val assignedWatermarksStream: DataStream[SensorReading] = dataStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })

    val resultDataStream: DataStream[SensorReading] = assignedWatermarksStream
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .reduce((curr, last) => SensorReading(curr.id, curr.timestamp, curr.temperature.max(last.temperature)))

    resultDataStream.print("resultDataStream")
    env.execute("window and watermark test")

  }
}