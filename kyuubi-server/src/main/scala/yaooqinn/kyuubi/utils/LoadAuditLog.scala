package yaooqinn.kyuubi.utils

import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._
import java.io.File
import java.nio.charset.StandardCharsets

object LoadAuditLog {

  def main(args: Array[String]) {
    val date = args(0)
    val operation_dir = args(1)
    val audit_merge = operation_dir + "/audit_merge_" + date
    println(audit_merge)

    val spark = SparkSession.builder.enableHiveSupport.appName("merge-load-auditlog").getOrCreate
    val buffer = new ArrayBuffer[String]
    val user_dirs = Files.list(Paths.get(operation_dir)).iterator
    user_dirs.foreach { p =>
      if (p.toFile.isDirectory) {
        println("user dir " + p)
        val audit_stream = Files.newDirectoryStream(p, s"*audit_$date*").iterator
        if (audit_stream.hasNext()) {
          val audit_file = audit_stream.next
          println(audit_file)
          buffer ++= Files.readAllLines(audit_file)
        }
      }
    }
    println(s"total query $buffer.size")
    Files.write(Paths.get(audit_merge), buffer, StandardCharsets.UTF_8)
    val local_audit = "file://" + audit_merge
    spark.sql(s"load data local inpath '$local_audit' into table bigolive.sparksql_job_audit partition(day='$date')")
    spark.stop
    System.exit(0)
  }

}