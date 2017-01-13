package org.xsl.common.db.mysql.dump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xsl.common.db.mysql.MysqlHelper;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.xsl.common.config.Constants.*;

/**
 * 主要完成的功能是定期导出某个表的全量数据，导出的数据放入到内存的缓存中，
 * 可以调用接口{@link #getData()}方法获取数据，详细配置请参考：*
 * Created by xiongsenlin on 15/9/27.
 */
public class MysqlReader implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MysqlReader.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private MysqlHelper mysqlHelper;

    /**
     * 同步的表名称
     */
    private String tableName;

    /**
     * 表示同步的表可以用哪个字段分为多个区间，通过字段划分区间后才方便分批查询，为了
     * 不影响查询效率，建议在该字段上建立索引。
     */
    private String partitionField;

    /**
     * 分区字段的类型，只支持int和datetime两种类型
     */
    private String partitionFieldType;

    /**
     * 表对应的主键
     */
    private List<String> primaryKesFields;

    /**
     * 表示读取数据库的线程数
     */
    private int threadPoolSize = 1;

    /**
     * 表示两次全量读取之间间隔的秒数，默认3天全量导入一次
     */
    private long readInterval = DAY_MILLISECONDS * 3;

    /**
     * 表示全量导入的次数，默认会无限循环导任意多次
     */
    private int fullDumpTimes = Integer.MAX_VALUE;

    /**
     * 表示全量导入的时间范围
     */
    private int intervalLower = -1, intervalUpper = -1;

    /**
     * 执行真正数据库数据查询的语句
     */
    private String querySql;

    /**
     * 表示要读取的字段，如果不配置则读取所有的字段
     */
    private Set<String> readFields;

    private AtomicInteger procDataNumber;
    private CountDownLatch countDownLatch;
    private ExecutorService executorService;

    private BlockingQueue<Segmentation> querySegmentation = new LinkedBlockingQueue<>();
    private BlockingQueue<Map<String, String>> dataList = new LinkedBlockingQueue<Map<String, String>>();

    public MysqlReader(Map<String, String> config) {
        this.mysqlHelper = new MysqlHelper(config);

        this.tableName = config.get(MYSQL_SYNC_TABLE_NAME);
        this.partitionField = config.get(MYSQL_PARTITION_FIELD);
        this.partitionFieldType = config.get(MYSQL_PARTITION_FIELD_TYPE);

        this.primaryKesFields = new ArrayList<>();
        String tmpKeys = config.get(MYSQL_PRIMARY_KEY_FIELDS);
        if (tmpKeys != null) {
            String[] keyItems = tmpKeys.split(",");
            for (String item : keyItems) {
                item = item.trim();
                this.primaryKesFields.add(item);
            }
        }

        if (this.primaryKesFields.isEmpty()) {
            throw new RuntimeException("Primary key fields is empty");
        }

        this.readFields = new HashSet<>();
        String fieldsStr = config.get(MYSQL_READ_FIELDS);
        if (fieldsStr != null) {
            String [] fieldItems = fieldsStr.split(",");
            for (String item : fieldItems) {
                item = item.trim();
                this.readFields.add(item);
            }
            for (String pKey : this.primaryKesFields) {
                this.readFields.add(pKey);
            }
        }

        String fields = "*";
        if (this.readFields.size() != 0) {
            StringBuilder sb = new StringBuilder();
            for (String item : this.readFields) {
                sb.append(item + ", ");
            }
            fields = sb.toString();
            fields = sb.substring(0, fields.length() - 2);
        }

        this.querySql = "select " + fields + " from " + tableName + " where "
                + partitionField + " >= {0} and " + partitionField + " < {1}";

        if (config.get(MYSQL_ALL_LOAD_FREQUENCY) != null) {
            float day = Float.parseFloat(config.get(MYSQL_ALL_LOAD_FREQUENCY).trim());
            this.readInterval = (int) (day * DAY_MILLISECONDS);
        }

        if (config.get(MYSQL_ALL_LOAD_TIMES) != null) {
            this.fullDumpTimes = Integer.parseInt(config.get(MYSQL_ALL_LOAD_TIMES).trim());
        }

        if (config.get(MYSQL_ALL_LOAD_INTERVAL) != null) {
            String tmp = config.get(MYSQL_ALL_LOAD_INTERVAL);
            String [] bounds = tmp.split("-");
            if (bounds.length == 2) {
                this.intervalLower = Integer.parseInt(bounds[0].trim());
                this.intervalUpper = Integer.parseInt(bounds[1].trim());
            }
            else {
                throw new IllegalArgumentException("Config [" + MYSQL_ALL_LOAD_INTERVAL + "] is invalid");
            }
        }

        if (config.get(MYSQL_FETCH_DATA_THREADS) != null) {
            this.threadPoolSize = Integer.parseInt(
                    config.get(MYSQL_FETCH_DATA_THREADS.trim()));
        }

        this.executorService = Executors.newFixedThreadPool(this.threadPoolSize + 1);
        this.executorService.execute(this);
    }

    /**
     * 向外部提供数据
     * @return
     */
    public Map<String, String> getData() {
        try {
            Map<String, String> data = this.dataList.poll(10, TimeUnit.MILLISECONDS);
            return data;
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        return null;
    }

    /**
     * 调度现成，定期执行一次全量查询，其它时间执行增量导入
     */
    public void run() {
        String format1 = "select {0}({1}) from {2}";
        String format2 = "select count(*) from {0} where {1} >= {2} and {3} <= {4}";

        String minSql = MessageFormat.format(format1, "min", this.partitionField, this.tableName);
        String maxSql = MessageFormat.format(format1, "max", this.partitionField, this.tableName);

        Long preTime = null;
        String min = null;
        List<String> preMinValues = new ArrayList<>();

        int dumpTimes = 0;
        while (++dumpTimes <= this.fullDumpTimes) {
            boolean fullLoad = false;
            if (preTime != null) {
                long now = System.currentTimeMillis();
                if (now - preTime >= this.readInterval && this.checkInterval()) {
                    preTime = now;
                    fullLoad = true;
                }
            } else {
                if (this.checkInterval()) {
                    fullLoad = true;
                    preTime = System.currentTimeMillis();
                }
            }

            /**
             * 下边界要视情况而定，如果是全量导入，则取数据库中的最小值，如果不是全量导入，则为上次全量导入的最大值
             */
            String max, count;
            try {
                if (fullLoad) {
                    min = this.mysqlHelper.getOneData(minSql);
                }
                max = this.mysqlHelper.getOneData(maxSql);

                if (min == null) {
                    min = this.getDefaultMaxMinValue("min", fullLoad);
                }
                if (max == null) {
                    max = this.getDefaultMaxMinValue("max", fullLoad);
                }

                if (this.partitionFieldType.equals("int")) {
                    count = this.mysqlHelper.getOneData(MessageFormat.format(format2,
                            this.tableName, this.partitionField, min, this.partitionField, max));
                } else {
                    count = this.mysqlHelper.getOneData(MessageFormat.format(format2,
                            this.tableName, this.partitionField, "'" + min + "'", this.partitionField, "'" + max + "'"));
                }
            } catch (Exception e) {
                logger.error("Get min, max or count error", e);
                continue;
            }

            String preProcessTimeStr = null;
            if (preTime != null) {
                preProcessTimeStr = sdf.format(new Date(preTime));
            }
            logger.info("[[[ Mysql reader thread heart beat, fullLoad: " + fullLoad
                    + " min: " + min + " max: " + max + " dataCount: " + count + " preProcessTime: " + preProcessTimeStr + " ]]]");

            this.getSegmentation(min, max, count);
            logger.info("[[[ jobListSize: " + this.querySegmentation.size() + " ]]]");

            int threadNumber = (this.querySegmentation.size() + 49) / 50;
            threadNumber = (threadNumber > this.threadPoolSize ? this.threadPoolSize : threadNumber);

            this.procDataNumber = new AtomicInteger(0);
            this.countDownLatch = new CountDownLatch(threadNumber);

            for (int i = 0; i < threadNumber; i++) {
                this.executorService.execute(new QueryRunner());
            }

            try {
                this.countDownLatch.await();
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (preMinValues.size() == 0) {
                preMinValues.add(min);
            }

            /**
             * 如果是全量导入或者前一次导入的数据量太大达到了5万的话，就不用回退重新导入了，
             * 否则的话每条数据在增量导入阶段，理论上会被导入30次，这样才能确保数据被放入到redis中
             */
            if (fullLoad || procDataNumber.get() >= 50000) {
                preMinValues.clear();
                preMinValues.add(max);
            }
            else {
                preMinValues.add(max);
                if (preMinValues.size() > 30) {
                    preMinValues.remove(0);
                }
            }

            min = preMinValues.get(0);
        }

        this.executorService.shutdownNow();
    }

    private boolean checkInterval() {
        if (this.intervalLower < 0) {
            return true;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        if (hour >= this.intervalLower && hour <= this.intervalUpper) {
            return true;
        }
        return false;
    }

    /**
     * 根据数据量的大小，将任务分成多个小块，每次执行1000条数据的查询任务
     * @param min
     * @param max
     * @param count
     */
    private void getSegmentation(String min, String max, String count) {
        List<String> result = new ArrayList<String>();

        int total;
        if (count == null) {
            total = 3000 * 3000;
        } else {
            total = Integer.parseInt(count);
        }

        if (total == 0) {
            return;
        }

        int segments = (total + 2999) / 3000;
        if (this.partitionFieldType.equals("int")) {
            int start = Integer.parseInt(min);
            int end = Integer.parseInt(max);
            int step = (end - start) / segments;
            for (int i = start; i < end && step >= 0; i += step) {
                result.add(i + "");
            }
            if (result.size() == 0) {
                result.add(start + "");
            }
            result.add(end + 1 + "");
        }

        else {
            try {
                long start = sdf.parse(min).getTime();
                long end = sdf.parse(max).getTime();
                long step = (end - start) / segments;
                for (long i = start; i < end && step >= 0; i += step) {
                    result.add("'" + sdf.format(new Date(i)) + "'");
                }
                if (result.size() == 0) {
                    result.add("'" + sdf.format(new Date(start)) + "'");
                }
                result.add("'" + sdf.format(new Date(end + 1000)) + "'");
            } catch (ParseException e) {
                logger.error("Date parse error", e);
            }
        }

        for (int i = 1; i < result.size(); i++) {
            String lower = result.get(i - 1);
            String upper = result.get(i);
            Segmentation segmentation = new Segmentation(lower, upper);
            this.querySegmentation.add(segmentation);
        }
    }

    /**
     * 获取默认的最大最小值
     * @param type
     * @param fullLoad
     * @return
     */
    private String getDefaultMaxMinValue(String type, boolean fullLoad) {
        if (type.equals("min")) {
            if (this.partitionFieldType.equals("int")) {
                if (fullLoad) {
                    return "-1";
                } else {
                    return Integer.MAX_VALUE + "";
                }
            }
            else {
                if (fullLoad) {
                    return "2010-12-12 00:00:00";
                } else {
                    return "2100-12-12 00:00:00";
                }
            }
        }
        else {
            if (this.partitionFieldType.equals("int")) {
                if (fullLoad) {
                    return Integer.MAX_VALUE + "";
                } else {
                    return "-1";
                }
            }
            else {
                if (fullLoad) {
                    return "2100-12-12 00:00:00";
                } else {
                    return "2010-12-12 00:00:00";
                }
            }
        }
    }

    /**
     * 根据分段，实际执行查询任务的现成类
     */
    private class QueryRunner implements Runnable {
        private final int sleepTime = 5000;

        public void run() {
            while (true) {
                try {
                    if (dataList.size() >= 50000) {
                        Thread.sleep(1000);
                        continue;
                    }

                    Segmentation segmentation = querySegmentation.poll(3, TimeUnit.SECONDS);
                    if (segmentation == null) {
                        countDownLatch.countDown();
                        break;
                    }

                    String sql = MessageFormat.format(querySql, segmentation.getLowerBound(), segmentation.getUpperBound());
                    try {
                        List<Map<String, String>> dataItems = mysqlHelper.query(sql);
                        if (dataItems.size() > 0) {
                            procDataNumber.addAndGet(dataItems.size());
                            dataList.addAll(dataItems);
                        }
                    } catch (Exception e) {
                        if (segmentation.getRetryTimes() < 3) {
                            segmentation.setRetryTimes(segmentation.getRetryTimes() + 1);
                            querySegmentation.add(segmentation);
                        }
                        else {
                            logger.info("Query mysql error", e);
                        }
                    }

                    logger.info("[[[ jobLeft: " + querySegmentation.size() +" querySql: " + sql
                            + " processedDataNumber: " + procDataNumber + " dataListSize: " + dataList.size() + " ]]]");

                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
