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

package yaooqinn.kyuubi.session

import java.io.{ File, IOException }
import java.util.Date
import java.util.concurrent._

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkConf

import yaooqinn.kyuubi.{ KyuubiSQLException, Logging }
import yaooqinn.kyuubi.operation.OperationManager
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.service.{ CompositeService, ServiceException }
import yaooqinn.kyuubi.spark.SparkSessionCacheManager
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.NamedThreadFactory
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.KyuubiSparkUtil
import yaooqinn.kyuubi.spark.SparkSessionWithUGI

/**
 * A SessionManager for managing [[KyuubiSession]]s
 */
private[kyuubi] class SessionManager private (
  name: String) extends CompositeService(name) with Logging {
  private val operationManager = new OperationManager()
  private val cacheManager = new SparkSessionCacheManager()
  private val handleToSession = new ConcurrentHashMap[SessionHandle, KyuubiSession]
  private var execPool: ThreadPoolExecutor = _
  private var isOperationLogEnabled = false
  private var operationLogRootDir: File = _
  private var resourcesRootDir: File = _
  private var checkInterval: Long = _
  private var sessionTimeout: Long = _
  private var checkOperation: Boolean = false
  private var shutdown: Boolean = false
  private val cleaner = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())

  def this() = this(classOf[SessionManager].getSimpleName)

  private def createExecPool(): Unit = {
    val poolSize = conf.get(ASYNC_EXEC_THREADS).toInt
    info("Background operation thread pool size: " + poolSize)
    val poolQueueSize = conf.get(ASYNC_EXEC_WAIT_QUEUE_SIZE).toInt
    info("Background operation thread wait queue size: " + poolQueueSize)
    val keepAliveTime = conf.getTimeAsSeconds(EXEC_KEEPALIVE_TIME)
    info("Background operation thread keepalive time: " + keepAliveTime + " seconds")
    val threadPoolName = classOf[KyuubiServer].getSimpleName + "-Background-Pool"
    execPool =
      new ThreadPoolExecutor(
        poolSize,
        poolSize,
        keepAliveTime,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable](poolQueueSize),
        new NamedThreadFactory(threadPoolName))
    execPool.allowCoreThreadTimeOut(true)
    checkInterval = conf.getTimeAsMs(FRONTEND_SESSION_CHECK_INTERVAL)
    sessionTimeout = conf.getTimeAsMs(FRONTEND_IDLE_SESSION_TIMEOUT)
    checkOperation = conf.get(FRONTEND_IDLE_SESSION_CHECK_OPERATION).toBoolean
  }

  private def initOperationLogRootDir(): Unit = {
    operationLogRootDir = new File(conf.get(LOGGING_OPERATION_LOG_DIR))
    isOperationLogEnabled = true
    if (operationLogRootDir.exists && !operationLogRootDir.isDirectory) {
      info("The operation log root directory exists, but it is not a directory: "
        + operationLogRootDir.getAbsolutePath)
      isOperationLogEnabled = false
    }
    if (!operationLogRootDir.exists) {
      if (!operationLogRootDir.mkdirs) {
        warn("Unable to create operation log root directory: "
          + operationLogRootDir.getAbsolutePath)
        isOperationLogEnabled = false
      }
    }

    if (isOperationLogEnabled) {
      if (operationLogRootDir.exists()) {
        info("Operation log root directory is created: " + operationLogRootDir.getAbsolutePath)
        try {
          //FileUtils.forceDeleteOnExit(operationLogRootDir)
        } catch {
          case e: IOException =>
            warn("Failed to schedule cleanup Kyuubi Server's operation logging root" +
              " dir: " + operationLogRootDir.getAbsolutePath, e)
        }
      } else {
        warn("Operation log root directory is not created: "
          + operationLogRootDir.getAbsolutePath)
      }
    }
  }

  private def initResourcesRootDir(): Unit = {
    resourcesRootDir = new File(conf.get(OPERATION_DOWNLOADED_RESOURCES_DIR))
    if (resourcesRootDir.exists() && !resourcesRootDir.isDirectory) {
      throw new ServiceException("The operation downloaded resources directory exists but is not" +
        s" a directory ${resourcesRootDir.getAbsolutePath}")
    }
    if (!resourcesRootDir.exists() && !resourcesRootDir.mkdirs()) {
      throw new ServiceException("Unable to create the operation downloaded resources directory" +
        s" ${resourcesRootDir.getAbsolutePath}")
    }

    try {
      FileUtils.forceDeleteOnExit(resourcesRootDir)
    } catch {
      case e: IOException =>
        warn("Failed to schedule clean up Kyuubi Server's root directory for downloaded" +
          " resources", e)
    }
  }

  /**
   * Periodically close idle sessions in 'spark.kyuubi.frontend.session.check.interval(default 6h)'
   */
  private def startTimeoutChecker(): Unit = {
    val interval: Long = math.max(checkInterval, 3000L)
    // minimum 3 seconds
    val timeoutChecker = new Runnable() {
      override def run(): Unit = {
        val current: Long = System.currentTimeMillis
        handleToSession.values.asScala.foreach { session =>
          val handle: SessionHandle = session.getSessionHandle
          val sessionUser = session.getUserName
          val noOperationTime = session.getNoOperationTime
          val lastAccessTime = session.getLastAccessTime
          try {
            if (cacheManager.getUserActiveSessionNum(sessionUser) < 0) {
              warn(s"session user $sessionUser can not found in cache manager, remove the session $handle")
              closeSession(handle)
            } else {
              info(s"""current session $handle, LastAccessTime $lastAccessTime, NoOperationTime $noOperationTime, User $sessionUser""")
              if (sessionTimeout > 0 && session.getLastAccessTime + sessionTimeout <= current
                && (!checkOperation || session.getNoOperationTime > sessionTimeout)) {
                warn("Session " + handle + " is Timed-out (last access: "
                  + new Date(session.getLastAccessTime) + ") and will be closed" + s" for user $sessionUser")
                closeSession(handle)
              } else {
                session.closeExpiredOperations
              }
            }
          } catch {
            case e: Throwable =>
              val err = KyuubiSparkUtil.exceptionString(e)
              warn(s"Exception is thrown in SessionManger cleaner for $handle\n$err", e)
          }
        }
      }
    }
    cleaner.scheduleWithFixedDelay(timeoutChecker, interval, interval, TimeUnit.MILLISECONDS)
  }

  private def sleepInterval(interval: Long): Unit = {
    try {
      Thread.sleep(interval)
    } catch {
      case _: InterruptedException =>
      // ignore
    }
  }

  private def cleanupLoggingRootDir(): Unit = {
    if (isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(operationLogRootDir)
      } catch {
        case e: Exception =>
          warn("Failed to cleanup root dir of KyuubiServer logging: "
            + operationLogRootDir.getAbsolutePath, e)
      }
    }
  }

  private def cleanupResourcesRootDir(): Unit = try {
    FileUtils.forceDelete(resourcesRootDir)
  } catch {
    case e: Exception =>
      warn("Failed to cleanup root dir of KyuubiServer logging: "
        + operationLogRootDir.getAbsolutePath, e)
  }

  override def init(conf: SparkConf): Unit = synchronized {
    this.conf = conf
    initResourcesRootDir()
    // Create operation log root directory, if operation logging is enabled
    if (conf.get(LOGGING_OPERATION_ENABLED).toBoolean) {
      initOperationLogRootDir()
    }
    createExecPool()
    addService(operationManager)
    addService(cacheManager)
    super.init(conf)
  }

  override def start(): Unit = {
    super.start()
    if (checkInterval > 0) {
      startTimeoutChecker()
    }
  }

  override def stop(): Unit = {
    super.stop()
    shutdown = true
    if (execPool != null) {
      execPool.shutdown()
      val timeout = conf.getTimeAsSeconds(ASYNC_EXEC_SHUTDOWN_TIMEOUT)
      try {
        execPool.awaitTermination(timeout, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException =>
          warn("ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e)
      }
      execPool = null
    }
    cleanupResourcesRootDir()
    //cleanupLoggingRootDir()
  }

  /**
   * Opens a new session and creates a session handle.
   * The username passed to this method is the effective username.
   * If withImpersonation is true (==doAs true) we wrap all the calls in HiveSession
   * within a UGI.doAs, where UGI corresponds to the effective user.
   */
  @throws[KyuubiSQLException]
  def openSession(
    protocol:          TProtocolVersion,
    username:          String,
    password:          String,
    ipAddress:         String,
    sessionConf:       Map[String, String],
    withImpersonation: Boolean): SessionHandle = {

    if (SparkSessionWithUGI.isPartiallyConstructed(username)) {
      throw new KyuubiSQLException(s"initializing sparkcontext for $username, please wait a moment")
    }

    if (!checkActiveUserSessionNum(username)) {
      throw new KyuubiSQLException(s"$username active session reach limit 3, can not create new one")
    }

    val kyuubiSession = new KyuubiSession(
      protocol,
      username,
      password,
      conf.clone(),
      ipAddress,
      withImpersonation,
      this,
      operationManager)
    info(s"Opening session for $username")

    kyuubiSession.setResourcesSessionDir(resourcesRootDir)
    if (isOperationLogEnabled) {
      kyuubiSession.setOperationLogSessionDir(operationLogRootDir)
    }
    kyuubiSession.open(sessionConf)

    val sessionHandle = kyuubiSession.getSessionHandle
    handleToSession.put(sessionHandle, kyuubiSession)
    KyuubiServerMonitor.getListener(kyuubiSession.getUserName).foreach {
      _.onSessionCreated(
        kyuubiSession.getIpAddress,
        sessionHandle.getSessionId.toString,
        kyuubiSession.getUserName)
    }

    sessionHandle
  }

  @throws[KyuubiSQLException]
  def getSession(sessionHandle: SessionHandle): KyuubiSession = {
    val session = handleToSession.get(sessionHandle)
    if (session == null) {
      throw new KyuubiSQLException("Invalid SessionHandle " + sessionHandle)
    }
    session
  }

  @throws[KyuubiSQLException]
  def closeSession(sessionHandle: SessionHandle) {
    val session = handleToSession.remove(sessionHandle)
    if (session == null) {
      throw new KyuubiSQLException(s"Session $sessionHandle does not exist!")
    }
    val sessionUser = session.getUserName
    KyuubiServerMonitor.getListener(sessionUser).foreach {
      _.onSessionClosed(sessionHandle.getSessionId.toString)
    }
    cacheManager.decrease(sessionUser)
    session.close()
    warn(s"close one session $sessionHandle for " + sessionUser)
  }

  def getOperationMgr: OperationManager = operationManager

  def getCacheMgr: SparkSessionCacheManager = cacheManager

  def getOpenSessionCount: Int = handleToSession.size

  def submitBackgroundOperation(r: Runnable): Future[_] = execPool.submit(r)

  def checkActiveUserSessionNum(username: String): Boolean = {
    info("check active session num for " + username)
    val times = cacheManager.getUserActiveSessionNum(username)
    info(s"$username current action session num " + times)
    if (times >= 3) {
      return false
    }
    true
  }

}
