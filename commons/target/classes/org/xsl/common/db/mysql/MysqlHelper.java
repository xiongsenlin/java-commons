package org.xsl.common.db.mysql;

import org.xsl.common.json.JsonHelper;
import org.xsl.common.config.Pair;
import org.xsl.common.string.StringUtils;
import java.sql.*;
import java.util.*;

import static org.xsl.common.config.Constants.*;

/**
 * 支持mysql常见的增删查改操作，数据库连接池对操作透明，如果确保对象不再使用的情况下，可以手动调用
 * {@link #close()}接口来释放连接池占有的所有资源
 *
 * Created by xiongsenlin on 15/7/8.
 */
public class MysqlHelper {
    private ConnectionPool connectionPool;

    public MysqlHelper(JsonHelper jsonHelper) {
        Map<String, String> conf = this.getConfig(jsonHelper);
        this.connectionPool = new ConnectionPool(conf);
    }

    /**
     * 通过map传递过来的参数初始化连接池
     * @param config config中包括的参数有：
     *        1. {@code mysql_con_url}           （必须）   数据库连接url
     *        2. {@code mysql_user_name}         （可选）   账号，如果在mysql_con_url中已经设置了账号，可以不用设置
     *        3. {@code mysql_pwd}               （可选）   密码，同上
     *        4. {@code mysql_pool_size}         （可选）   连接池的容量，如果不传递，默认值：5
     *        5. {@code mysql_pool_max_idle}     （可选）   连接池中最大的空闲连接数量，默认值：5
     *        6. {@code mysql_pool_min_idle}     （可选）   连接池中最小的空闲连接数量，默认值：1
     *        7. {@code mysql_pool_max_total}    （可选）   连接池中允许存在的最大连接数量，默认值：15
     */
    public MysqlHelper(Map<String, String> config) {
        this.connectionPool = new ConnectionPool(config);
    }

    /**
     * 获取数据库连接
     * @return
     */
    public Connection getConnection() {
        Connection conn = this.connectionPool.getConn(false);
        if (conn == null) {
            throw new RuntimeException("Can not get connection from pool");
        }

        return conn;
    }

    /**
     * 初始化自定义PreparedStatement
     * @return
     */
    public PreparedStatement initStatement(Connection conn, String sql) throws SQLException {
        if (conn == null) {
            throw new RuntimeException("Can not get connection from pool");
        }

        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            throw e;
        } finally {
            conn.close();
        }
    }

    /**
     * 设置PreparedStatement参数
     */
    public void setStmtParamByField(PreparedStatement stmt, SqlField field) throws SQLException {
        if (field == null) {
            return;
        }

        String valueStr = field.fieldValueStr;
        String valueType = field.fieldValueType;
        Integer paramIndex = field.paramIndex;

        if (StringUtils.isEmpty(valueStr) || StringUtils.isEmpty(valueType) || paramIndex == null) {
            throw new RuntimeException("Invalid param field");
        }

        switch (valueType.toLowerCase()) {
            case "int":
                stmt.setInt(paramIndex, Integer.valueOf(valueStr));
                break;
            case "long":
                stmt.setLong(paramIndex, Long.valueOf(valueStr));
                break;
            case "double":
                stmt.setDouble(paramIndex, Double.valueOf(valueStr));
                break;
            case "float":
                stmt.setFloat(paramIndex, Float.valueOf(valueStr));
                break;
            case "string":
                stmt.setString(paramIndex, valueStr);
                break;
            case "timestamp":
                stmt.setTimestamp(paramIndex, Timestamp.valueOf(valueStr));
                break;
            case "boolean":
                stmt.setBoolean(paramIndex, Boolean.valueOf(valueStr));
                break;
            default:
                throw new RuntimeException("Could not recognize " +
                        "the type [" + valueType + "] when set PreparedStatement by field");
        }
    }

    /**
     * 执行查询操作，返回的每条数据对应一个{@link Map}，所以不能保证字段的顺序，所有数据
     * 都被转换成{@link String}，客户端使用的时候需要手动转换成对应的类型
     * @param sql  需要执行的查询语句
     * @return     返回的结果是一个list，每一条记录是一个map
     */
    public List<Map<String, String>> query(String sql) throws SQLException {
        Connection conn = this.connectionPool.getConn(false);
        if (conn == null) {
            throw new RuntimeException("Can not get connection from pool");
        }

        List<Map<String, String>> result = new ArrayList<>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                Map<String, String> dataItem = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rs.getMetaData().getColumnName(i);
                    String columnValue = rs.getString(i);
                    dataItem.put(columnName, columnValue);
                }
                result.add(dataItem);
            }
            return result;
        } catch (Exception e) {
            throw e;
        } finally {
            conn.close();
        }
    }

    /**
     * 执行查询操作，返回的每条数据对应一个{@link List&lt;Pair&gt;}对象，与sql中的查询顺序保持一致,
     * 其中{@link Pair#value1}表示字段名称，{@link Pair#value2}表示字段值
     * @param sql
     * @return
     */
    public List<List<Pair<String, String>>> queryInOrder(String sql) throws SQLException {
        Connection conn = this.connectionPool.getConn(false);
        if (conn == null) {
            throw new RuntimeException("Can not get connection from pool");
        }

        List<List<Pair<String, String>>> result = new ArrayList<>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<Pair<String, String>> dataItems = new LinkedList<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rs.getMetaData().getColumnName(i);
                    String columnValue = rs.getString(i);
                    dataItems.add(new Pair<>(columnName, columnValue));
                }
                result.add(dataItems);
            }
            return result;
        }
        catch (Exception e) {
            throw e;
        } finally {
            conn.close();
        }
    }

    /**
     * @see #query(String), 此方法与其最大的区别就是不会将字段的值转换为{@link String}, 而是保留原来的类型
     * @param sql
     * @return
     */
    public List<Map<String, Object>> queryPrimitives(String sql) throws SQLException {
        Connection conn = this.connectionPool.getConn(false);
        if (conn == null) {
            throw new RuntimeException("Can not get connection from pool");
        }

        List<Map<String, Object>> result = new ArrayList<>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                Map<String, Object> dataItem = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rs.getMetaData().getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    dataItem.put(columnName, columnValue);
                }
                result.add(dataItem);
            }
            return result;
        }
        catch (Exception e) {
            throw e;
        } finally {
            conn.close();
        }
    }

    /**
     * 此接口主要方便用在某些特殊的场景，查询结果只有一条数据，并且只有一列的情况
     * 例如查询总的数据条数，查询最大值，最小值等类型的查询
     * @param sql
     * @return
     */
    public String getOneData(String sql) throws SQLException {
        Connection conn = this.connectionPool.getConn(false);
        if (conn == null) {
            throw new RuntimeException("Can not get connection from pool");
        }

        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            int columnCount = rs.getMetaData().getColumnCount();
            if (columnCount == 1 && rs.next()) {
                return rs.getString(1);
            }
            else {
                return null;
            }
        } catch (Exception e) {
            throw e;
        } finally {
            conn.close();
        }
    }

    /**
     * 支持 insert, update, delete三种操作
     * @param sql
     * @return
     */
    public boolean update(String sql) throws SQLException {
        Connection conn = this.connectionPool.getConn(false);
        if (conn == null) {
            throw new RuntimeException("Can not get connection from pool");
        }

        try {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
            return true;
        }
        catch (SQLException e) {
            throw e;
        } finally {
            conn.close();
        }
    }

    /**
     * 释放连接池占用的所有资源
     * @throws Exception
     */
    public void close() throws Exception {
        this.connectionPool.close();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.close();
    }

    private Map<String, String> getConfig(JsonHelper helper) {
        if (helper == null) {
            throw new RuntimeException("App config helper not set");
        }

        Map<String, String> conf = new HashMap<>();

        String conUrl = helper.getStringValue(MYSQL_CON_URL);
        if (conUrl == null) {
            throw new RuntimeException("Config [" + MYSQL_CON_URL + "] is null");
        }

        String pwd = helper.getStringValue(MYSQL_PWD);
        String userName = helper.getStringValue(MYSQL_USER_NAME);
        String poolSize = helper.getStringValue(MYSQL_POOL_SIZE);
        String maxPoolTotal = helper.getStringValue(MYSQL_POOL_MAX_TOTAL);
        String maxPoolIdle = helper.getStringValue(MYSQL_POOL_MAX_IDLE);
        String minPoolIdle = helper.getStringValue(MYSQL_POOL_MIN_IDLE);

        conf.put(MYSQL_CON_URL, conUrl);
        conf.put(MYSQL_USER_NAME, userName);
        conf.put(MYSQL_PWD, pwd);
        conf.put(MYSQL_POOL_SIZE, poolSize);
        conf.put(MYSQL_POOL_MAX_TOTAL, maxPoolTotal);
        conf.put(MYSQL_POOL_MAX_IDLE, maxPoolIdle);
        conf.put(MYSQL_POOL_MIN_IDLE, minPoolIdle);

        return conf;
    }

    public static class SqlField {
        public String fieldValueStr;
        public String fieldValueType;
        public Integer paramIndex;

        public SqlField(String valueStr, String valueType) {
            this.fieldValueStr = valueStr;
            this.fieldValueType = valueType;
        }

        public SqlField(String valueStr, String valueType, Integer index) {
            this.fieldValueStr = valueStr;
            this.fieldValueType = valueType;
            this.paramIndex = index;
        }
    }
}
