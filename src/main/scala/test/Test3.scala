package test


import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.common.functions.AggregateFunction

import scala.collection.mutable

object Test3 {
  def main(args: Array[String]): Unit = {
    // 环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dstream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val textWithTsDstream: DataStream[(String, Long, Int)] = dstream.map { text =>
      val arr: Array[String] = text.split(" ")
      (arr(0), arr(1).toLong, 1)
    }
    val textWithEventTimeDstream: DataStream[(String, Long, Int)] = textWithTsDstream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(1000)) {
        override def extractTimestamp(element: (String, Long, Int)): Long = {
          element._2
        }
      })
    val textKeyStream: KeyedStream[(String, Long, Int), Tuple] = textWithEventTimeDstream.keyBy(0)
    textKeyStream.print("textkey:")
    val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] =
      textKeyStream
        .window(TumblingEventTimeWindows.of(Time.seconds(2)))

    val groupDstream: DataStream[mutable.HashSet[Long]] = windowStream.fold(new mutable.HashSet[Long]()) {
      case (set, (key, ts, count)) => set += ts
    }

    // val groupDstream: DataStream[mutable.HashSet[Long]] = windowStream.aggregate(new TimestampAggregator())

    groupDstream.print("window::::").setParallelism(1)

    env.execute()


    class TimestampAggregator extends AggregateFunction[(Any, Long, Int), mutable.HashSet[Long], mutable.HashSet[Long]] {
      override def createAccumulator(): mutable.HashSet[Long] = new mutable.HashSet[Long]()

      override def add(value: (Any, Long, Int), accumulator: mutable.HashSet[Long]): mutable.HashSet[Long] = {
        accumulator += value._2 // Collect timestamp `ts` into the accumulator
        accumulator
      }

      override def getResult(accumulator: mutable.HashSet[Long]): mutable.HashSet[Long] = accumulator

      override def merge(a: mutable.HashSet[Long], b: mutable.HashSet[Long]): mutable.HashSet[Long] = a ++ b
    }


  }


}
