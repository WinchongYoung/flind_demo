package sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestSQL {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getWrappedStreamExecutionEnvironment
    val sql =
      """
        |CREATE TABLE user1 (
        |  id BIGINT,
        |  name VARCHAR(45),
        |  PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |  'connector' = 'jdbc',
        |  'url' = 'localhost',
        |  'driver' = 'com.mysql.jdbc.Driver',
        |  'table-name' = 'user1',
        |  'username' = 'root',
        |  'password' = '123456',
        |  'sink.buffer-flush.max-rows' = '10',
        |  'sink.buffer-flush.interval' = '2s',
        |  'sink.max-retries' = '2'
        |);
        |""".stripMargin

  }

}
