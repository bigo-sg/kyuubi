package yaooqinn.kyuubi.operation

import org.apache.spark.sql.RuntimeConfig
import yaooqinn.kyuubi.Logging

object SqlChecker extends Logging {
  val DEFAULT_COMMON_EVENT_TABLE = "bigo_show_user_event,like_user_event,cube_show_user_event,talk_show_user_event";
  val partitions_regex = "PartitionCount: (\\d+),".r

  def checkPartitionExceed(inputTables: Seq[String], conf: RuntimeConfig, plan: String): Boolean = {
    val tables_limit = conf.get("spark.kyuubi.limit.tables", DEFAULT_COMMON_EVENT_TABLE)
    val tableSet = Set(tables_limit.split(","): _*)
    var needVerify = false
    for (t <- inputTables) {
      info("table " + t)
      if (tableSet.contains(t.replaceAll("_orc$", "").replaceAll("_v2$", "").replaceAll("_hour_orc$", ""))) {
        needVerify = true
      }
    }
    if (needVerify) {
      val num = getPartNum(plan)
      if (num.isDefined) {
        info("found partition " + num.get)
        val limit = getPartLimit(conf)
        if (num.get.toInt > limit) {
          return false;
        }
      }
    }
    true
  }

  def getPartLimit(conf: RuntimeConfig) = {
    conf.get("spark.kyuubi.query.partition.limit", "10").toInt
  }

  def getPartNum(plan: String) = {
    partitions_regex.findFirstMatchIn(plan) match {
      case Some(m) => Some(m.group(1))
      case None    => None
    }
  }

}