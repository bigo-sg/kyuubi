package yaooqinn.kyuubi.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._

object LoadFlumeAuditLog {

  def main(args: Array[String]) {
    val date = args(0)
    val operation_dir = args(1)
    val flume_dir = "/flume/bigolive/sparksql_job_audit_flume/day=" + date
    val audit_merge = operation_dir + "/flume_audit_merge_" + date
    println(audit_merge)

    val conf = new Configuration
    val localfs = FileSystem.get(URI.create("file:///"), conf)
    val hdfs = FileSystem.get(conf)
    val flume_files = hdfs.listStatus(new Path(flume_dir))
    val out = localfs.create(new Path(audit_merge))
    flume_files.foreach { file =>
      val in = hdfs.open(file.getPath)
      IOUtils.copyBytes(in, out, conf, false)
      in.close
    }
    out.close

    val spark = SparkSession.builder.enableHiveSupport.appName("merge-load-flume-auditlog").getOrCreate
    val local_flume_audit = "file://" + audit_merge
    spark.sql(s"load data local inpath '$local_flume_audit' into table bigolive.sparksql_job_audit partition(day='$date')")
    spark.stop
    localfs.delete(new Path(audit_merge), true)
    System.exit(0)
  }

}