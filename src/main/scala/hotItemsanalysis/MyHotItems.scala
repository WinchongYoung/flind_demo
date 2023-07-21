package hotItemsanalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util.function.Consumer
import scala.collection.mutable.ListBuffer
import scala.math.Ordering

/**
 * 滑动窗口统计近一小时热门商品，每5分钟更新一次
 */

// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.数据源获取
    val dataStream = env.readTextFile("src/main/resources/UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior.equals("pv"))
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new MyAggregateFunction(), new MyWindowFunction())
      .keyBy(_.windowEnd)
      .process(new MyProcessFun(5))

    // 4. sink：控制台输出
    processedStream.print()

    env.execute("hot items job")
  }

  /**
   * IN 输入
   * ACC 累加器的类型
   * OUT 输出
   */
  class MyAggregateFunction() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  /**
   * IN – The type of the input value.
   * OUT – The type of the output value.
   * KEY – The type of the key
   * W  - The type of the window
   */
  class MyWindowFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
    }
  }

  /**
   * <K> – Type of the key. MyWindowFunction的key
   * <I> – Type of the input elements.MyWindowFunction的out
   * <O> – Type of the output elements.
   *
   * @param size topN
   */
  class MyProcessFun(size: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      itemState = getRuntimeContext.getListState(new ListStateDescriptor("item-state", classOf[ItemViewCount]))
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      itemState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val listBuffer: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]
      itemState.get().forEach(new Consumer[ItemViewCount] {
        override def accept(t: ItemViewCount): Unit = {
          listBuffer += t
        }
      })
      val sortedItems = listBuffer.sortBy(_.count)(Ordering.Long.reverse).take(size)
      val sb = new StringBuilder
      sb.append("+++++++++++++截至时间 ").append(new Timestamp(timestamp - 1)).append("++++++++++").append("\r\n")
      for (i <- sortedItems.indices) {
        val item = sortedItems(i)
        sb.append("No.").append(i + 1).append(" 商品ID:").append(item.itemId).append(" pv").append(item.count).append("\r\n")
      }
      out.collect(sb.toString())
      itemState.clear()
    }
  }
}

