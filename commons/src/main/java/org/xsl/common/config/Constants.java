package org.xsl.common.config;

/**
 * Created by xiongsenlin on 15/6/2.
 */
public class Constants {
    private Constants() {}

    /**
     * MYSQL相关的一些配置项
     */
    public static final String MYSQL_USER_NAME                     = "mysql_user_name";
    public static final String MYSQL_PWD                           = "mysql_pwd";
    public static final String MYSQL_CON_URL                       = "mysql_con_url";
    public static final String MYSQL_POOL_SIZE                     = "mysql_pool_size";
    public static final String MYSQL_POOL_MAX_IDLE                 = "mysql_pool_max_idle";
    public static final String MYSQL_POOL_MIN_IDLE                 = "mysql_pool_min_idle";
    public static final String MYSQL_POOL_MAX_TOTAL                = "mysql_pool_max_total";


    /**
     * hbase相关的一些配置项
     */
    public static final String HBASE_MASTER                        = "hbase.master";
    public static final String HBASE_ZK_QUORUM                     = "hbase.zookeeper.quorum";
    public static final String HBASE_ZK_CLIENT_PORT                = "hbase.zookeeper.property.clientport";
}
