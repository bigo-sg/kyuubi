package yaooqinn.kyuubi.operation;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SemanticAnalyzerHiveDriverRunHook implements HiveDriverRunHook {
    private static final SessionState.LogHelper console = SessionState.getConsole();
    private static String[] PARTITION_KEYS_RESTRICT = {"day"};
    private static Map<String, List<String>> tablePartition = new ConcurrentHashMap<>();
    private static String hintContent = "示例如下：" +
            "(1)left join类型，外层的on条件只下推到内层的右表，外层的where条件只下推到内层的左表。" +
            "正确示例：select id, event_id from vlog.testparser t1 left join vlog.testparser2 t2 " +
            "on t1.event_id=t2.event_id and t2.day='YYYY-MM-DD' where t1.day='YYYY-MM-DD';  " +
            "(2)right join类型，外层的on条件只下推到内层的左表，外层的where条件只下推到内层的右表。" +
            "正确示例：select id, event_id from vlog.testparser t1 right join vlog.testparser2 t2 " +
            "on t1.event_id=t2.event_id and t1.day='YYYY-MM-DD' where t2.day='YYYY-MM-DD';  " +
            "(3)inner join类型，外层的where条件和on条件均下推到内层，若表含day分区字段，在外层或内层指定均可。   " +
            "(4)full join类型，外层的where条件和on条件均不下推到内层，若内层的表含day分区，需在内层指定day字段。";

    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception{
        HiveAuthenticationProvider authProvider = SessionState.get().getAuthenticator();
        String username = authProvider.getUserName();
        if(! username.equals("hadoop")){
            prePartitionKeyLimitPolicy(hookContext);
            System.out.println("hiveDriverRunHook check succeed!");
        }
    }

    public void postDriverRun(HiveDriverRunHookContext hookContext){

    }

    private void prePartitionKeyLimitPolicy(HiveDriverRunHookContext hookContext) throws Exception{
        String sql = hookContext.getCommand();
        processSQL(sql);
    }

    private void processOtherTypeSelect(String sql) throws Exception{
        String str="";
        String sql_format = sql.toLowerCase().replace("select*", "select *");
        List<String> lst = Arrays.asList(sql_format.split(" "));
        int index = lst.indexOf("select");
        if(index != -1) {
            for (int i = index; i < lst.size(); i++) {
                str += lst.get(i) + " ";
            }
        }
        processSQL(str);
    }

    public void processSQL(String sql) throws Exception{
        if(sql == null || sql == "")
            return;
        List<String> lst = Arrays.asList(sql.toUpperCase().split(" "));
        if(lst.contains("CREATE") || lst.contains("INSERT")){
            processOtherTypeSelect(sql);
        }
        Statement stmt = null;
        try{
            stmt = CCJSqlParserUtil.parse(sql);
        }catch (Exception e){
            return;
        }

        if(stmt instanceof Select) {
            // fetch all tables' partition, store in tablePartition.
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List<String> tableList = tablesNamesFinder.getTableList(stmt);
            for (String str : tableList) {
                if (str.contains(".")) {
                    String schemaName = str.substring(0, str.indexOf("."));
                    String tableName = str.substring(str.lastIndexOf(".") + 1);
                    tablePartition.put(str, fetchPartitionKeys(schemaName, tableName));
                }
            }
            Select select = (Select) stmt;
            SelectBody selectBody = select.getSelectBody();
            processSelectBody(selectBody);

            List<WithItem> withItems = select.getWithItemsList();
            if (withItems == null)
                return;
            for (WithItem withItem : withItems) {
                processSelectBody(withItem.getSelectBody());
            }
        }
    }

    private List<String> fetchPartitionKeys(String schemaName, String tableName) throws Exception{
        List<String> partitionKeys = new ArrayList<>();
        if(tableName == null || schemaName == null)
            return partitionKeys;

        HiveConf hiveConf = new HiveConf();
        HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
        if(client.getTable(schemaName, tableName) == null)
            return partitionKeys;

        List<FieldSchema> partitionFieldSchema = client.getTable(schemaName, tableName).getPartitionKeys();
        for(FieldSchema field : partitionFieldSchema){
            if(field.getName() != null)
                partitionKeys.add(field.getName().toLowerCase());
        }
//        System.out.println("table: "+schemaName+"."+tableName+", partitionKeys: "+partitionKeys);
        return partitionKeys;
    }

    private static void processSelectBody(SelectBody selectBody) throws Exception{
//        System.out.println("processing selectBody: " + selectBody.toString());
        if (selectBody instanceof PlainSelect) {
            processPlainSelect((PlainSelect) selectBody);
        }else if (selectBody instanceof SetOperationList){
            processUnion((SetOperationList) selectBody);
        }
    }

    private static void processFromItem(FromItem fromItem) throws Exception{
//        System.out.println("processing fromItem: " + fromItem.toString());
        if (fromItem instanceof SubJoin) {
            SubJoin subJoin = (SubJoin) fromItem;
            if (subJoin.getJoinList() != null) {
                List<Join> joinList = subJoin.getJoinList();
                for(Join join : joinList){
                    processFromItem(join.getRightItem());
                }
            }
            if (subJoin.getLeft() != null) {
                processFromItem(subJoin.getLeft());
            }
        } else if (fromItem instanceof SubSelect) {
            SubSelect subSelect = (SubSelect) fromItem;
            if (subSelect.getSelectBody() != null) {
                processSelectBody(subSelect.getSelectBody());
            }
        }
    }

    private static void processUnion(SetOperationList setOperationList) throws Exception{
//        System.out.println("processing union...");
        List<SelectBody> selectBodies = setOperationList.getSelects();
        for(SelectBody selectBody : selectBodies){
            processSelectBody(selectBody);
        }
    }

    private static void processPlainSelect(PlainSelect plainSelect) throws Exception{
        Set<String> outerWhereKeys = new HashSet<>();
        Expression where = plainSelect.getWhere();
        if(where != null){
            outerWhereKeys = getWhereKeyFromClause(where);
        }
        String tableName = null;

        Set<String> leftJoinKeys = new HashSet<>();
        Set<String> leftWhereKeys = new HashSet<>();
        String leftAlias = null;
        String leftTableName = null;
        Map<String, String> map = getPlainSelectInfo(plainSelect);
        leftAlias = map.get("alias");
        leftTableName = map.get("name");

        // process fromItem
        processFromItem(plainSelect.getFromItem());

        // select statement with no join clause
        if(plainSelect.getJoins() == null){
            if(plainSelect.getFromItem() instanceof SubSelect){
                // recurse
                processFromItem(plainSelect.getFromItem());
            }else{
//                System.out.println("------normal case------");
                tableName = map.get("name");

                Set<String> whereKeys = null;
                if(where != null){
                    whereKeys = getWhereKeyFromClause(where);
                }
                handleNormal(tableName, whereKeys);
            }

        } else if (plainSelect.getJoins().size() > 0) {
            List<Join> joins = plainSelect.getJoins();
            FromItem leftFromItem = plainSelect.getFromItem();
            if(leftFromItem instanceof SubSelect && leftFromItem != null){
                SubSelect leftSubSelect = (SubSelect) leftFromItem;
                SelectBody leftFromSelectBody = leftSubSelect.getSelectBody();

                if(leftFromSelectBody instanceof PlainSelect){
                    PlainSelect leftPlainSelect = (PlainSelect) leftFromSelectBody;
                    Expression leftWhere = leftPlainSelect.getWhere();
                    if(leftWhere != null){
                        leftWhereKeys = getWhereKeyFromClause(leftWhere);
                    }
                }else if(leftFromSelectBody instanceof SetOperationList){
                    processUnion((SetOperationList) leftFromSelectBody);
                }
            }

            for (Join join : joins) {
                String rightAlias = null;
                String rightTableName = null;
                FromItem joinRightItem = join.getRightItem();
                if(joinRightItem == null)
                    continue;

                if(joinRightItem.getAlias() == null){
                    rightAlias = joinRightItem.toString();
                }else {
                    rightAlias = joinRightItem.getAlias().getName();
                }
//                System.out.println("joinRightItem: " + joinRightItem);
                if(joinRightItem instanceof SubSelect){
                    // recurse
                    SubSelect subSelect = (SubSelect) joinRightItem;
                    SelectBody selectBody = subSelect.getSelectBody();

                    if(selectBody instanceof PlainSelect){
                        PlainSelect plainSelect1 = (PlainSelect) selectBody;
                        if(plainSelect1.getFromItem() instanceof SubSelect){
                            // join嵌套子查询的子查询，是临时中间表而非join的左/右表，不做下推，做normal限制
                            processSelectBody(selectBody);
                        }else{
                            Table table = (Table) plainSelect1.getFromItem();
                            rightTableName = table.getSchemaName() + "." + table.getName();
                        }
                    }else if(selectBody instanceof SetOperationList){
                        processUnion((SetOperationList)selectBody);
                    }

                }else if (joinRightItem instanceof Table){
                    Table table = (Table) joinRightItem;
                    rightTableName = table.getSchemaName() + "." + table.getName();
                }

                Set<String> rightJoinKeys = new HashSet<>();
                Set<String> rightWhereKeys = new HashSet<>();

                // joinKey
                Set<String> joinKeys = new HashSet<>();
                if(join.getOnExpression() != null)
                    joinKeys = getJoinKeyFromClause(join.getOnExpression());

                if (joinRightItem instanceof SubSelect && joinRightItem != null){
                    SubSelect subSelect = (SubSelect) joinRightItem;
                    SelectBody selectBody = subSelect.getSelectBody();
                    if(selectBody instanceof PlainSelect){
                        PlainSelect plainSelect2 = (PlainSelect) selectBody;
                        Expression whereClause = plainSelect2.getWhere();
                        if(whereClause != null) {
                            rightWhereKeys = getWhereKeyFromClause(whereClause);
                        }
                    }else if(selectBody instanceof SetOperationList){
                        processUnion((SetOperationList) selectBody);
                    }
                }
                if(join.isInner()){
                    // inner join 左右表均谓词下推，
                    for(String whereKey : outerWhereKeys){
                        String str = whereKey.toLowerCase();
                        if(str.contains(".")){
                            String tbName = str.substring(0, str.indexOf("."));
                            String key = str.substring(str.lastIndexOf(".") + 1);
                            if(tbName.equals(leftAlias.toLowerCase()) || tbName.equals(leftTableName.toLowerCase()))
                                leftWhereKeys.add(key);
                            if(tbName.equals(rightAlias.toLowerCase()) || tbName.equals(rightTableName.toLowerCase()))
                                rightWhereKeys.add(key);
                        } else {
                            leftWhereKeys.add(str);
                            rightWhereKeys.add(str);
                        }
                    }
                    for(String joinKey : joinKeys){
                        String str = joinKey.toLowerCase();
                        if(!str.contains(".")){
                            leftJoinKeys.add(str);
                            rightJoinKeys.add(str);
                        }else {
                            String tbName = str.substring(0, str.indexOf("."));
                            String key = str.substring(str.lastIndexOf(".") + 1);
                            if(tbName.equals(leftAlias.toLowerCase()) || tbName.equals(leftTableName.toLowerCase())){
                                leftJoinKeys.add(key);
                            }else if (tbName.equals(rightAlias.toLowerCase()) || tbName.equals(rightTableName.toLowerCase())) {
                                rightJoinKeys.add(key);
                            }
                        }
                    }
                    handleJoin(leftTableName, rightTableName, leftWhereKeys, rightWhereKeys, leftJoinKeys, rightJoinKeys);

                }else if(join.isLeft()) {
                    // on条件不下推到左表，下推到右表；where条件不下推到右表，下推到左表
                    for(String whereKey : outerWhereKeys){
                        String str = whereKey.toLowerCase();
                        if(str.contains(".")){
                            String tbName = str.substring(0, str.indexOf("."));
                            if (tbName.equals(leftAlias.toLowerCase()) || tbName.equals(leftTableName.toLowerCase()))
                                leftWhereKeys.add(str.substring(str.lastIndexOf(".")+1));
                        } else {
                            leftWhereKeys.add(str);
                        }
                    }
                    for(String joinKey : joinKeys){
                        String str = joinKey.toLowerCase();
                        if(!str.contains(".")){
                            rightJoinKeys.add(str);
                        }else {
                            String tbName = str.substring(0, str.indexOf("."));
                            String key = str.substring(str.lastIndexOf(".") + 1);
                            if (tbName.equals(rightAlias) || tbName.equals(rightTableName)) {
                                rightJoinKeys.add(key);
                            }
                        }
                    }
                    handleJoin(leftTableName, rightTableName, leftWhereKeys, rightWhereKeys, leftJoinKeys, rightJoinKeys);

                }else if (join.isRight()) {
                    // on条件不下推到右表，下推到左表；where条件不下推到左表，下推到右表
                    for(String whereKey : outerWhereKeys){
                        String str = whereKey.toLowerCase();
                        if(str.contains(".")){
                            String tbName = str.substring(0, str.indexOf("."));
                            if (tbName.equals(rightAlias.toLowerCase()) || tbName.equals(rightTableName.toLowerCase()))
                                rightWhereKeys.add(str.substring(str.lastIndexOf(".")+1));
                        } else {
                            rightWhereKeys.add(str);
                        }
                    }

                    for(String joinKey : joinKeys){
                        String str = joinKey.toLowerCase();
                        if(!str.contains(".")){
                            leftJoinKeys.add(str);
                        }else {
                            String tbName = str.substring(0, str.indexOf("."));
                            if (tbName.equals(leftAlias.toLowerCase()) || tbName.equals(leftTableName.toLowerCase())) {
                                leftJoinKeys.add(str.substring(str.lastIndexOf(".") + 1));
                            }
                        }
                    }
                    handleJoin(leftTableName, rightTableName, leftWhereKeys, rightWhereKeys, leftJoinKeys, rightJoinKeys);

                }else if(join.isFull()){
                    // full join 均不谓词下推
                    handleJoin(leftTableName, rightTableName, leftWhereKeys, rightWhereKeys, leftJoinKeys, rightJoinKeys);
                }else if(joinRightItem instanceof SubSelect){
                    // select id, day from vlog.testparser, (select * from (select id,day from vlog.testparser2 where day='')t) as data1；
                    processFromItem(joinRightItem);
                }
            }
        }
    }

    private static Set<String> getJoinKeyFromClause(Expression expression){
        Set<String> result = new HashSet<>();
        if (expression == null)
            return result;
//        Expression expr = CCJSqlParserUtil.parseCondExpression(strClause);
        expression.accept(new ExpressionVisitorAdapter() {
            @Override
            protected void visitBinaryExpression(BinaryExpression expr) {
                if (expr instanceof ComparisonOperator) {
                    Expression leftExpression = expr.getLeftExpression();
                    Expression rightExpression = expr.getRightExpression();
                    if(leftExpression != null && rightExpression != null) {
                        String str1 = leftExpression.toString().toLowerCase();
                        String str2 = rightExpression.toString().toLowerCase();
                        // 只取on具体值的key, 如t1.day='2019-01-01'，t1.day=t2.day不算在内
                        if (!str1.substring(str1.indexOf(".") + 1).equals(str2.substring(str2.indexOf(".") + 1))) {
                            result.add(str1);
                        }
                    }
                }
                super.visitBinaryExpression(expr);
            }
        });
        return result;
    }

    private static Set<String> getWhereKeyFromClause(Expression expression){
        Set<String> result = new HashSet<>();
        if (expression == null)
            return result;
        String whereClause = expression.toString().toLowerCase();
        Pattern pattern = Pattern.compile(".*(\\d){4}-(\\d){2}-(\\d){2}.*");
        Matcher matcher = pattern.matcher(whereClause);
        if(whereClause.contains("day") && matcher.matches())
            result.add("day");

        expression.accept(new ExpressionVisitorAdapter() {
            @Override
            protected void visitBinaryExpression(BinaryExpression expr) {
                // where 默认都是值，如where t1.day='2019-01-01'
                if (expr instanceof ComparisonOperator) {
                    Expression leftExpression = expr.getLeftExpression();
                    if(leftExpression == null)
                        return;

                    String str = leftExpression.toString().toLowerCase();
                    if (str.contains(".")) {
                        result.add(str.substring(str.lastIndexOf(".") + 1));
                    } else {
                        result.add(str);
                    }
                }
                super.visitBinaryExpression(expr);
            }
            @Override
            public void visit(Between expr) {
                Expression leftExpression = expr.getLeftExpression();
                if(leftExpression == null)
                    return;

                String str = leftExpression.toString().toLowerCase();
                if (str.contains(".")) {
                    result.add(str.substring(str.lastIndexOf(".") + 1));
                } else {
                    result.add(expr.getLeftExpression().toString());
                }
            }
        });
        return result;
    }

    private static Map<String, String> getPlainSelectInfo(PlainSelect plainSelect) {
        String tableAlias = "";
        String tableName = "";
        Map<String, String> map = new ConcurrentHashMap<>();
        FromItem fromItem = plainSelect.getFromItem();
        if(plainSelect == null || fromItem == null)
            return map;

        if(fromItem.getAlias() == null){
            tableAlias = fromItem.toString();
        }else {
            tableAlias = fromItem.getAlias().getName();
        }
        if(fromItem instanceof Table){
            Table table = (Table) fromItem;
            tableName = table.getSchemaName() + "." + table.getName();
        }
        map.put("alias", tableAlias);
        map.put("name", tableName);

        return map;
    }

    private static void handleNormal(String tableName, Set<String> whereKeys) throws Exception{
        List<String> partition = new ArrayList<>();
        if(tableName != null && tablePartition.containsKey(tableName))
            partition = tablePartition.get(tableName);

        String hint = "本表%s含分区字段%s，查询需带分区字段%s，或请在正确的位置带分区字段。" + hintContent;
        for(String str : PARTITION_KEYS_RESTRICT){
            if((whereKeys == null || ! (whereKeys.contains(str) || whereKeys.contains(str.toUpperCase())))
                    && (partition.contains(str) || partition.contains(str.toUpperCase())))
                throw new JSQLParserException(String.format(hint, tableName, str, str));
        }
    }

    private static void handleJoin(String leftTableName, String rightTableName, Set<String> leftWhereKeys, Set<String> rightWhereKeys,
                                   Set<String> leftJoinKeys, Set<String> rightJoinKeys) throws Exception{
        List<String> leftTablePartition = new ArrayList<>();
        List<String> rightTablePartition = new ArrayList<>();

        if(leftTableName != null && tablePartition.containsKey(leftTableName))
            leftTablePartition = tablePartition.get(leftTableName);
        if(rightTableName != null && tablePartition.containsKey(rightTableName)) {
            rightTablePartition = tablePartition.get(rightTableName);
        }

        String hint = "本表%s含分区字段%s，查询需带分区字段%s，或请在正确的位置带分区字段。" + hintContent;
        for(String key : PARTITION_KEYS_RESTRICT){
            String str = key.toLowerCase();
            if((rightWhereKeys == null || !(rightWhereKeys.contains(str) || rightWhereKeys.contains(str.toUpperCase())
                    || rightJoinKeys.contains(str) || rightJoinKeys.contains(str.toLowerCase())))
                    && (rightTablePartition.contains(str) || rightTablePartition.contains(str.toUpperCase()))) {

                throw new JSQLParserException(String.format(hint, rightTableName, str, str));
            }
        }

        for(String str : PARTITION_KEYS_RESTRICT){
            if((leftWhereKeys == null || ! (leftWhereKeys.contains(str) || leftWhereKeys.contains(str.toUpperCase())
                    || leftJoinKeys.contains(str) || leftJoinKeys.contains(str.toUpperCase())))
                    && (leftTablePartition.contains(str) || leftTablePartition.contains(str.toUpperCase()))) {

                throw new JSQLParserException(String.format(hint, leftTableName, str, str));
            }
        }
    }

}
