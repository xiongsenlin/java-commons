package org.xsl.common.model;

import java.text.SimpleDateFormat;

/**
 * Created by xiongsenlin on 15/6/2.
 */
public class Constants {
    private Constants() {}

    public static SimpleDateFormat SDF                       = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String JSON_STR_EMPTY_VALUE                = "{}";
    public static String DATETIME_DEFAULT_VALUE              = "0000-00-00 00:00:00";
    public static String NUMERIC_DATA_DEFAULT_VALUE          = "0";
    public static String STRING_DEFAULT_VALUE                = "";

    /**
     * MYSQL相关的一些配置项
     */
    public static String MYSQL_USER_NAME                     = "mysql_user_name";
    public static String MYSQL_PWD                           = "mysql_pwd";
    public static String MYSQL_CON_URL                       = "mysql_con_url";
    public static String MYSQL_POOL_SIZE                     = "mysql_pool_size";
    public static String MYSQL_POOL_MAX_IDLE                 = "mysql_pool_max_idle";
    public static String MYSQL_POOL_MIN_IDLE                 = "mysql_pool_min_idle";
    public static String MYSQL_POOL_MAX_TOTAL                = "mysql_pool_max_total";
}
