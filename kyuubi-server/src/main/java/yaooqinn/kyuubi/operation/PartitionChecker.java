package yaooqinn.kyuubi.operation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

import yaooqinn.kyuubi.KyuubiSQLException;

public class PartitionChecker {
	static Context ctx;
	static SessionState ss;
	static HiveConf hiveconf;
	static boolean inited = false;

	static Set<String> limitTableSet;
	static Set<String> commonEventTableSet;
	static int maxCommonPartitionLimit;

	private final static Pattern datePattern = Pattern.compile("\\d{4}-?\\d{2}-?\\d{2}");
	private final static int MAX_PARTITION_LIMIT = 100;
	private final static int DEFAULT_MAX_COMMON_EVENT_PARTITION_LIMIT = 10;
	private final static String MAX_COMMON_EVENT_PARTITION_LIMIT_CONFIG_NAME = "max.common.event.partition.limit";
	private final static String LIMIT_TABLE_CONFIG_NAME = "common.event.limit.table";
	private final static String DEFAULT_LIMIT_TABLE_CONFIG = "bigo_show_user_event,like_user_event,cube_show_user_event";
	private final static String COMMON_EVENT_TABLE_CONFIG_NAME = "common.event.table.list";
	private final static String DEFAULT_COMMON_EVENT_TABLE_CONFIG_NAME = "bigo_show_user_event,like_user_event,cube_show_user_event,talk_show_user_event";
	private final static String COMMON_EVENT_FLAG_FIELD_NAME = "event_id";
	private final static List<String> DATABASE_LIMIT = Arrays.asList("tmp", "report_tb", "mediate_tb");

	public static boolean isInited() {
		return inited;
	}

	public static void initContext(Configuration conf)
			throws IOException, LockException, ClassNotFoundException, SQLException {
		hiveconf = new HiveConf();
		hiveconf.addResource(conf);
		hiveconf.set("hive.exec.dynamic.partition.mode", "nonstrict");
		ss = SessionState.start(hiveconf);
		ss.initTxnMgr(hiveconf);
		ctx = new Context(hiveconf);
		postConfigSet(conf);
		inited = true;
		Path sessionPath = SessionState.getHDFSSessionPath(conf);
		FileSystem fs = sessionPath.getFileSystem(conf);
		fs.setPermission(sessionPath, new FsPermission("777"));
	}

	public static void setSessionState() {
		SessionState.setCurrentSessionState(ss);
	}

	public static void setSessionCurTime() {
		ss.setupQueryCurrentTimestamp();
	}

	public static void check(String sql) throws Exception {
		if (sql.toLowerCase().startsWith("set ")) {
			return;
		}
		ASTNode tree = ParseUtils.parse(sql, ctx);
		// tree = ParseUtils.findRootNonNullToken(tree);
		checkCreateTable(sql, tree);

		BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(new QueryState(hiveconf), tree);
		sem.analyze(tree, ctx);
		sem.validate();
		Set<ReadEntity> readEntities = sem.getInputs();
		postDayPartitionMaxLimitPolicy(readEntities);
		postCommonEventTableLimitPolicy(readEntities);
		postCommonEventFlagPartitionPolicy(readEntities);
	}

	private static void checkCreateTable(String sql, ASTNode ast) throws KyuubiSQLException {
		List<String> lst = Arrays.asList(sql.toLowerCase().trim().split("\\s* \\s*"));
		// create external table is ok.
		int index = lst.indexOf("create");
		int index2 = lst.indexOf("external");
		if (index + 1 == index2)
			return;

		String astDBName = "";
		boolean hasLocation = false;
		if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
			hasLocation = hasLocationField(ast);
			ASTNode tabNameNode = (ASTNode) ast.getChild(0);
			if (tabNameNode.getToken().getType() == HiveParser.TOK_TABNAME) {
				if (tabNameNode.getChildCount() != 0) {
					String str = tabNameNode.getChild(0).toString();
					if (str.contains("."))
						astDBName = str.substring(0, str.indexOf("."));
				}
			}
		}
		if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
			if (hasLocation && DATABASE_LIMIT.contains(astDBName))
				throw new KyuubiSQLException(
						String.format("在%s库建内表不可以指定location，去掉location或构建外表（create external table ......）", astDBName));
		}
	}

	static boolean hasLocationField(ASTNode ast) {
		if (ast == null)
			return false;
		if (ast.getToken().getType() == HiveParser.TOK_TABLELOCATION)
			return true;

		int childCount = ast.getChildCount();
		for (int i = 0; i < childCount; i++) {
			ASTNode node = (ASTNode) ast.getChild(i);
			if (hasLocationField(node))
				return true;
		}
		return false;
	}

	private static void postConfigSet(Configuration conf) {
		String limitTable = conf.get(LIMIT_TABLE_CONFIG_NAME, DEFAULT_LIMIT_TABLE_CONFIG);
		limitTableSet = new HashSet<String>();
		for (String table : limitTable.split(",")) {
			limitTableSet.add(table.trim());
		}

		String commonTable = conf.get(COMMON_EVENT_TABLE_CONFIG_NAME, DEFAULT_COMMON_EVENT_TABLE_CONFIG_NAME);
		commonEventTableSet = new HashSet<String>();
		for (String table : commonTable.split(",")) {
			commonEventTableSet.add(table.trim());
		}

		maxCommonPartitionLimit = conf.getInt(MAX_COMMON_EVENT_PARTITION_LIMIT_CONFIG_NAME,
				DEFAULT_MAX_COMMON_EVENT_PARTITION_LIMIT);
	}

	private static void postCommonEventTableLimitPolicy(Set<ReadEntity> readEntities) throws KyuubiSQLException {
		for (ReadEntity readEntity : readEntities) {
			Table table = readEntity.getTable();
			Partition part = readEntity.getPartition();
			if (table != null && part != null) {
				String entityTable = table.getTableName();
				if (limitTableSet.contains(entityTable)) {
					throw new KyuubiSQLException(String.format(
							"通用事件表%s查询效率较低，需要查询当天之前数据的请使用%s_orc表, 需要查询实时数据的请使用%s_v2表（查询通用事件表需指定具体分区条件：where event_id='xxxx'）",
							entityTable, entityTable, entityTable));
				}
			}
		}
	}

	private static void postDayPartitionMaxLimitPolicy(Set<ReadEntity> readEntities) throws KyuubiSQLException {
		Map<String, HashSet<String>> partMap = new HashMap<String, HashSet<String>>();
		for (ReadEntity readEntity : readEntities) {
			Partition part = readEntity.getPartition();
			if (part != null) {
				String tableName = part.getTable().getTableName();
				String partName = part.getName();
				Matcher m = datePattern.matcher(partName);
				if (m.find()) {
					String day = m.group(0);
					if (partMap.containsKey(tableName)) {
						partMap.get(tableName).add(day);
					} else {
						partMap.put(tableName, new HashSet<String>());
						partMap.get(tableName).add(day);
					}
				}
			}
		}

		for (String key : partMap.keySet()) {
			if (partMap.get(key).size() > MAX_PARTITION_LIMIT) {
				throw new KyuubiSQLException(String.format("对表%s查询扫描的日期分区数为%d个,超出最大限制分区数%d。", key,
						partMap.get(key).size(), MAX_PARTITION_LIMIT));
			}
		}
	}

	private static void postCommonEventFlagPartitionPolicy(Set<ReadEntity> readEntities) throws KyuubiSQLException {
		Map<String, HashSet<String>> partMap = new HashMap<String, HashSet<String>>();
		for (ReadEntity readEntity : readEntities) {
			Partition part = readEntity.getPartition();
			if (part != null) {
				String tableName = part.getTable().getTableName();
				String partPath = part.getPartitionPath().getName();
				if (commonEventTableSet
						.contains(tableName.replaceAll("_orc$", "").replaceAll("_v2$", "").replaceAll("_hour_orc$", ""))
						&& partPath.startsWith(COMMON_EVENT_FLAG_FIELD_NAME + "=")) {
					if (partMap.containsKey(tableName)) {
						partMap.get(tableName).add(partPath);
					} else {
						partMap.put(tableName, new HashSet<String>());
						partMap.get(tableName).add(partPath);
					}
				}
			}
		}

		for (String key : partMap.keySet()) {
			if (partMap.get(key).size() > maxCommonPartitionLimit) {
				throw new KyuubiSQLException(
						String.format("通用事件表%s必须指定事件ID分区条件（如event_id=\"0103004\"），并且指定事件ID分区数不能超过最大限制:%d", key,
								maxCommonPartitionLimit));
			}
		}
	}

}
