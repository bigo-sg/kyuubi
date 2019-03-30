/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ui

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{ KyuubiSparkUtil, SparkConf }
import org.apache.spark.scheduler.{ SparkListener, SparkListenerJobStart }
import org.apache.spark.sql.internal.SQLConf
import java.io.File
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import org.apache.commons.io.IOUtils
import org.apache.commons.io.FileUtils
import yaooqinn.kyuubi.Logging
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import scala.collection.mutable.LinkedHashMap
import java.time.LocalDate

class KyuubiServerListener(conf: SparkConf, userAuditDir: File) extends SparkListener with Logging {

  private[this] var onlineSessionNum: Int = 0
  private[this] val sessionList = new mutable.LinkedHashMap[String, SessionInfo]
  private[this] val executionList = new mutable.LinkedHashMap[String, ExecutionInfo]
  private[this] val retainedStatements =
    conf.getInt(SQLConf.THRIFTSERVER_UI_STATEMENT_LIMIT.key, 200)
  private[this] val retainedSessions = conf.getInt(SQLConf.THRIFTSERVER_UI_SESSION_LIMIT.key, 200)
  private[this] var totalRunning = 0
  private val audit_name_template = "audit_xx.log"

  def getOnlineSessionNum: Int = synchronized { onlineSessionNum }

  def getTotalRunning: Int = synchronized { totalRunning }

  def getSessionList: Seq[SessionInfo] = synchronized { sessionList.values.toSeq }

  def getSession(sessionId: String): Option[SessionInfo] = synchronized {
    sessionList.get(sessionId)
  }

  def getExecutionList: Seq[ExecutionInfo] = synchronized { executionList.values.toSeq }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    for {
      props <- Option(jobStart.properties)
      groupIdKey <- props.stringPropertyNames().asScala
        .filter(_.startsWith(KyuubiSparkUtil.getJobGroupIDKey))
      groupId <- Option(props.getProperty(groupIdKey))
      (_, info) <- executionList if info.groupId == groupId
    } {
      info.jobId += jobStart.jobId.toString
      info.groupId = groupId
    }
  }

  def onSessionCreated(ip: String, sessionId: String, userName: String = "UNKNOWN"): Unit = {
    synchronized {
      val info = new SessionInfo(sessionId, System.currentTimeMillis, ip, userName)
      sessionList.put(sessionId, info)
      onlineSessionNum += 1
      trimSessionIfNecessary()
    }
  }

  def onSessionClosed(sessionId: String): Unit = synchronized {
    sessionList(sessionId).finishTimestamp = System.currentTimeMillis
    onlineSessionNum -= 1
    trimSessionIfNecessary()
  }

  def onStatementStart(
    id:        String,
    sessionId: String,
    statement: String,
    groupId:   String,
    userName:  String = "UNKNOWN"): Unit = synchronized {
    val info = new ExecutionInfo(statement, sessionId, System.currentTimeMillis, userName)
    info.state = ExecutionState.STARTED
    executionList.put(id, info)
    trimExecutionIfNecessary()
    sessionList(sessionId).totalExecution += 1
    executionList(id).groupId = groupId
    totalRunning += 1
  }

  def onStatementParsed(id: String, executionPlan: String): Unit = synchronized {
    executionList(id).executePlan = executionPlan
    executionList(id).state = ExecutionState.COMPILED
  }

  def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit = {
    synchronized {
      executionList(id).finishTimestamp = System.currentTimeMillis
      executionList(id).detail = errorMessage
      executionList(id).state = ExecutionState.FAILED
      totalRunning -= 1
      writeAuditLog(executionList(id))
      trimExecutionIfNecessary()
    }
  }

  def onStatementFinish(id: String): Unit = synchronized {
    executionList(id).finishTimestamp = System.currentTimeMillis
    executionList(id).state = ExecutionState.FINISHED
    totalRunning -= 1
    writeAuditLog(executionList(id))
    trimExecutionIfNecessary()
  }

  private def trimExecutionIfNecessary(): Unit = {
    if (executionList.size > retainedStatements) {
      val toRemove = math.max(retainedStatements / 10, 1)
      executionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
        executionList.remove(s._1)
      }
    }
  }

  private def trimSessionIfNecessary(): Unit = {
    if (sessionList.size > retainedSessions) {
      val toRemove = math.max(retainedSessions / 10, 1)
      sessionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
        sessionList.remove(s._1)
      }
    }
  }

  private def writeAuditLog(info: ExecutionInfo) {
    val log = generateLog(info)
    if (userAuditDir != null) {
      val userAuditLog = new File(userAuditDir, audit_name_template.replace("xx", LocalDate.now.toString))
      FileUtils.write(userAuditLog, log, true)
    } else {
      warn("not found user audit log for " + log)
    }
  }

  private def generateLog(info: ExecutionInfo) = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val statment = info.statement.replace("\n", " ").replace("\"", "'".replace("\t", " "))
    val begin_ts = info.startTimestamp
    val finished_ts = info.finishTimestamp
    val state = info.state
    val user = info.userName
    val dt_begin = new Timestamp(begin_ts).toLocalDateTime
    val dt_end = new Timestamp(finished_ts).toLocalDateTime
    val dur = java.time.Duration.between(dt_begin, dt_end).getSeconds
    val map = new LinkedHashMap[String, String]
    map += ("job_state" -> state.toString)
    map += ("user_name" -> user)
    map += ("start_time" -> dt_begin.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
    map += ("end_time" -> dt_end.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
    map += ("query" -> statment)
    map += ("duration" -> dur.toString)
    mapper.writeValueAsString(map) + "\n"
  }
}
