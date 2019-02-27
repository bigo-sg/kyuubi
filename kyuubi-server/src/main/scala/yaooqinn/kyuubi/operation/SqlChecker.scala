package yaooqinn.kyuubi.operation

import org.apache.spark.sql.RuntimeConfig

object SqlChecker {
  val partitions_regex = "PartitionCount: (\\d+),".r

  def getPartLimit(conf: RuntimeConfig) = {
    conf.get("query.partition.limit", "10").toInt
  }

  def getPartNum(plan: String) = {
    partitions_regex.findFirstMatchIn(plan) match {
      case Some(m) => Some(m.group(1))
      case None      =>  None
    }
  }

}