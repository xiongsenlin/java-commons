package org.xsl.common.config;

/**
 * Created by xiongsenlin on 15/6/2.
 */
public class Constants {
    private Constants() {}

    public static final int DAY_MILLISECONDS                       = 24 * 3600 * 1000;


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

    public static final String MYSQL_SYNC_TABLE_NAME               = "mysql_sync_table_name";
    public static final String MYSQL_BINLOG_TABLE_NAME_FILTER      = "mysql_binlog_table_name_filter";
    public static final String MYSQL_PRIMARY_KEY_FIELDS            = "mysql_primary_key_fields";
    public static final String MYSQL_PARTITION_FIELD               = "mysql_partition_field";
    public static final String MYSQL_PARTITION_FIELD_TYPE          = "mysql_partition_field_type";
    public static final String MYSQL_FULL_DUMP_FREQUENCY           = "mysql_full_dump_frequency";
    public static final String MYSQL_BATCH_INSERT_NUMBER           = "mysql_batch_insert_number";
    public static final String MYSQL_ALL_LOAD_FREQUENCY            = "mysql_all_load_frequency";
    public static final String MYSQL_ALL_LOAD_TIMES                = "mysql_all_load_times";
    public static final String MYSQL_ALL_LOAD_INTERVAL             = "mysql_all_load_interval";
    public static final String MYSQL_READ_FIELDS                   = "mysql_read_fields";
    public static final String MYSQL_FETCH_DATA_THREADS            = "mysql_fetch_data_threads";
    public static final String MYSQL_PROCESS_FLAG_SUFFIX           = "mysql_process_flag_key_suffix";
    public static final String MYSQL_IS_DUMP_RIGHT_NOW             = "mysql_is_dump_right_now";
    public static final String MYSQL_JOB_QUEUE_LENGTH              = "mysql_job_queue_length";
    public static final String MYSQL_DUMP_CONFIG                   = "mysql_dump_config";
    public static final String MYSQL_QUERY_SQL                     = "mysql_query_sql";

    /**
     * hbase相关的一些配置项
     */
    public static final String HBASE_MASTER                        = "hbase.master";
    public static final String HBASE_ZK_QUORUM                     = "hbase.zookeeper.quorum";
    public static final String HBASE_ZK_CLIENT_PORT                = "hbase.zookeeper.property.clientport";
}
