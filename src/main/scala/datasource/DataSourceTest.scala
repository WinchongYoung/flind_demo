package datasource

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import scala.util.Random

object DataSourceTest {
  def main(args: Array[String]): Unit = {

    // 创建一个流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new MySourceFunction)

    var sum: Int = 0
    val stream2 = stream.map(x => {
      sum = sum + x
      (x, sum)
    })
    stream2.print()

    // 执行任务
    env.execute("stream word count job")
  }
}

class MySourceFunction() extends SourceFunction[Int]() {

  var isCancel = false

  override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
    val random: Random = new Random()
    while (!isCancel) {
      sourceContext.collect(random.nextInt(10))
      Thread.sleep(100L)
    }
  }

  override def cancel(): Unit = {
    isCancel = true
  }
}