package hotItemsanalysis

import api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Date
import java.text.SimpleDateFormat

/**
 * 计算一小时内UV
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // 用相对路径定义数据源
    val inputStream = env.readTextFile("src/main/resources/UserBehavior.csv").
      map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
    val dataStream = inputStream
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计pv操作
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    dataStream.print()
    env.execute("uv job")
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCountNew, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCountNew]): Unit = {
    // 定义一个scala set，用于保存所有的数据userId并去重
    var idSet = Set[Long]()
    // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    out.collect(UvCountNew(dateFormat.format(new Date(window.getEnd)), idSet.size))
  }
}


class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound: Long = 60 * 1000 // 延时为 1 分钟
  var maxTs: Long = Long.MinValue // 观察到的最大时间戳

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(r: SensorReading, previousTS: Long) = {
    maxTs = maxTs.max(r.timestamp)
    r.timestamp
  }
}

case class UvCountNew(windowEnd: String, uvCount: Long)
