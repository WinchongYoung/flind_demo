package test

import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.immutable

object keyby_datastream {


  case class UserGrade(user_id: String, weekday: String, grade: Int)

  case class UserGradeList(user_id: String, gradeList: List[Int])

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketStream: DataStream[String] = env.socketTextStream("localhost", 8888)

    val userGradeStream: DataStream[UserGrade] = socketStream.map((data: String) => {
      val user_array: Array[String] = data.split(",")
      UserGrade(user_array(0), user_array(1), user_array(2).toInt)
    })

    val keyWondowStream: WindowedStream[UserGrade, String, TimeWindow] =
      userGradeStream
        .keyBy((_: UserGrade).user_id)
      .timeWindow(Time.seconds(5))

    //    Base interface for functions that are evaluated over keyed (grouped) windows.
    //    trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable
    //    val resultStream: DataStream[(String, List[Int])] = keyWondowStream.apply(new WindowFunction[UserGrade, (String, List[Int]), String, TimeWindow] {
    //      override def apply(key: String, window: TimeWindow, input: Iterable[UserGrade], out: Collector[(String, List[Int])]): Unit = {
    //        var gradeList: ListBuffer[Int] = ListBuffer[Int]()
    //        for (data <- input) {
    //          gradeList += data.grade
    //        }
    //        out.collect((key,gradeList.toList.sorted))
    //      }
    //    })


    //        val resultStream: DataStream[String] = keyWondowStream.apply(new WindowFunction[UserGrade, String, String, TimeWindow] {
    //          override def apply(key: String, window: TimeWindow, input: Iterable[UserGrade], out: Collector[String]): Unit = {
    //            var gradeList: ListBuffer[Int] = ListBuffer[Int]()
    //            for (data <- input){
    //              gradeList+=data.grade
    //            }
    //            val result: String = key + "===" + gradeList.toList.sorted.toString
    //            out.collect(result)
    //          }
    //        })


    val resultStream: DataStream[UserGradeList] = keyWondowStream.process(new ProcessWindowFunction[UserGrade, UserGradeList, String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[UserGrade], out: Collector[UserGradeList]): Unit = {
        var gradeList: ListBuffer[Int] = ListBuffer[Int]()
        for (data <- elements) {
          gradeList += data.grade
        }
        out.collect(UserGradeList(key, gradeList.toList.sorted))
      }
    })

    resultStream.print("===")
    env.execute("process apply function")
  }
}