package sql

import org.apache.flink.types.Row
import org.apache.flink.table.api.bridge.scala.tableConversions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object MySQLSourceExample {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 定义MySQL连接参数
    val mysqlUsername = "root"
    val mysqlPassword = "123456"
    val mysqlDbName = "test"
    val mysqlTableName = "user1"
    val mysqlUrl = s"jdbc:mysql://localhost:3306/$mysqlDbName?user=$mysqlUsername&password=$mysqlPassword"

    // 在Flink SQL中注册MySQL数据源
    tableEnv.executeSql(
      s"""
      CREATE TABLE user1 (
        id INT,
        name STRING
      ) WITH (
        'connector' = 'jdbc',
        'url' = '$mysqlUrl',
        'table-name' = '$mysqlTableName',
        'driver' = 'com.mysql.jdbc.Driver',
        'username' = '$mysqlUsername',
        'password' = '$mysqlPassword'
      )
    """)

    // 执行查询操作
    val result = tableEnv.sqlQuery("SELECT * FROM user1")

    // 将结果打印输出
    import org.apache.flink.streaming.api.scala._
    result.toAppendStream[Row].print()
    // result.toAppendStream

    // 执行任务
    env.execute("MySQL Source Example")
  }
}
