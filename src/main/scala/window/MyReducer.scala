package window

import api.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction

class MyReducer extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature))
  }
}