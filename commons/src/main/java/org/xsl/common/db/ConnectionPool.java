package org.xsl.common.db;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static org.xsl.common.model.Constants.*;

/**
 * Created by xiongsenlin on 15/7/8.
 */
public final class ConnectionPool {
    private static final int DEFAULT_POOL_SIZE = 5;
    private static final int DEFAULT_MAX_TOTAL = DEFAULT_POOL_SIZE * 3;
    private static final int DEFAULT_MAX_IDLE  = DEFAULT_POOL_SIZE;
    private static final int DEFAULT_MIN_IDLE  = 1;

    private static Logger logger = Logger.getLogger(ConnectionPool.class);
    private BasicDataSource dataSource;

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
    public ConnectionPool(Map<String, String> config) {
        this.initDatasource(config);
    }

    /**
     * 获取Connection对象，对于使用完毕的Connection对象，一定要记得close掉，
     * 不然连接池资源很快就会被耗尽，建议在finally语句中完成此操作
     * @return
     */
    public Connection getConn(boolean autoCommit) {
        Connection connection = null;
        if (dataSource != null) {
            try {
                connection = dataSource.getConnection();
            } catch (Exception e) {
                logger.error("Get db connection error", e);
                return null;
            }

            try {
                connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                logger.error("Set autocommit error", e);
                return null;
            }
        }
        return connection;
    }

    public void initDatasource(Map<String, String> conf) {
        this.checkInitParams(conf);

        BasicDataSource bds = new BasicDataSource();
        bds.setDriverClassName("com.mysql.jdbc.Driver");

        String url = conf.get(MYSQL_CON_URL);
        bds.setUrl(url);
        if (!(url.contains("user") && url.contains("password"))) {
            bds.setUsername(conf.get(MYSQL_USER_NAME));
            bds.setPassword(conf.get(MYSQL_PWD));
        }

        int poolSize = DEFAULT_POOL_SIZE;
        if (conf.containsKey(MYSQL_POOL_SIZE)) {
            poolSize = Integer.parseInt(conf.get(MYSQL_POOL_SIZE));
        }

        int maxTotal = DEFAULT_MAX_TOTAL;
        if (conf.containsKey(MYSQL_POOL_MAX_TOTAL)) {
            maxTotal = Integer.parseInt(conf.get(MYSQL_POOL_MAX_TOTAL));
        }

        int maxIdle = DEFAULT_MAX_IDLE;
        if (conf.containsKey(MYSQL_POOL_MAX_IDLE)) {
            maxIdle = Integer.parseInt(conf.get(MYSQL_POOL_MAX_IDLE));
        }

        int minIdle = DEFAULT_MIN_IDLE;
        if (conf.containsKey(MYSQL_POOL_MIN_IDLE)) {
            minIdle = Integer.parseInt(conf.get(MYSQL_POOL_MIN_IDLE));
        }

        bds.setInitialSize(poolSize);
        bds.setMaxTotal(maxTotal);
        bds.setMaxIdle(maxIdle);
        bds.setMinIdle(minIdle);
        bds.setMaxWaitMillis(30 * 1000);

        this.dataSource = bds;
    }

    private void checkInitParams(Map<String, String> conf) {
        if (conf == null) {
            throw new RuntimeException("mysql config is null");
        }

        String conUrl;
        if (!conf.containsKey(MYSQL_CON_URL)) {
            throw new RuntimeException("Mysql connection url is null");
        }
        else {
            conUrl = conf.get(MYSQL_CON_URL);
        }

        int index = conUrl.indexOf("?");
        if (index >= 0) {
            String params = conUrl.substring(index + 1);
            String[] items = params.split("&");
            for (String item : items) {
                String[] kv = item.split("=");
                if (kv.length == 2) {
                    if ("user".equals(kv[0])) {
                        conf.put(MYSQL_USER_NAME, kv[1]);
                    }
                    else if ("password".equals(kv[0])) {
                        conf.put(MYSQL_PWD, kv[1]);
                    }
                }
            }
        }

        if (!conf.containsKey(MYSQL_USER_NAME)) {
            throw new RuntimeException("Mysql user name is null");
        }
        if (!conf.containsKey(MYSQL_PWD)) {
            throw new RuntimeException("Mysql pwd is null");
        }
    }

    public void close() throws Exception {
        this.dataSource.close();
    }
}