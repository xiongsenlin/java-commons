package org.xsl.common.db.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by xiongsenlin on 15/7/8.
 */
public abstract class HbaseAPIBase {
    private int retryTimes = 1;

    private HTablePool tablePool;
    private Configuration configuration;

    public HbaseAPIBase(Map<String, String> conf) throws Exception {
        this.configuration = HBaseConfiguration.create();

        for (Map.Entry<String, String> entry : conf.entrySet()) {
            this.configuration.set(entry.getKey(), entry.getValue());
        }

        this.tablePool = new HTablePool(configuration, 10);
    }

    /**
     * 向Hbase中插入一条数据
     * @param tableName
     * @param hbaseDataItemBase
     * @return
     */
    public void put(String tableName, HbaseModelBase hbaseDataItemBase) throws Exception {
        HTable table = (HTable) this.tablePool.getTable(tableName);

        Put put = hbaseDataItemBase.getPut();
        try {
            table.put(put);
        } catch (Exception e) {
            throw e;
        } finally {
            table.close();
        }
    }

    /**
     * 向HBase中插入一批数据
     * @param tableName
     * @param hbaseDataItemBaseList
     * @return
     */
    public void batchPut(String tableName, List<HbaseModelBase> hbaseDataItemBaseList) throws Exception {
        HTable table = (HTable) this.tablePool.getTable(tableName);

        List<Put> puts = new ArrayList<>();
        for(HbaseModelBase item : hbaseDataItemBaseList) {
            Put put = item.getPut();
            puts.add(put);
        }

        try {
            table.put(puts);
        } catch (Exception e) {
            throw e;
        } finally {
            table.close();
        }
    }

    /**
     * 从Hbase中删除一条数据
     * @param tableName
     * @param hbaseDataItemBase
     * @return
     */
    public void delete(String tableName, HbaseModelBase hbaseDataItemBase) throws Exception {
        HTable table = (HTable) this.tablePool.getTable(tableName);

        Delete delete = hbaseDataItemBase.getDelete();
        try {
            table.delete(delete);
        } catch (Exception e) {
            throw e;
        } finally {
            table.close();
        }
    }

    /**
     * 从Hbase中一次删除一批数据
     * @param tableName
     * @param hbaseDataItemBaseList
     * @return
     */
    public void batchDelete(String tableName, List<HbaseModelBase> hbaseDataItemBaseList) throws Exception {
        HTable table = (HTable) this.tablePool.getTable(tableName);

        List<Delete> deletes = new ArrayList<>();
        for(HbaseModelBase item : hbaseDataItemBaseList) {
            Delete delete = item.getDelete();
            deletes.add(delete);
        }

        try {
            table.delete(deletes);
        } catch (Exception e) {
            throw e;
        } finally {
            table.close();
        }
    }

    /**
     * 根据row key从hbase中获取一条数据
     * @param tableName
     * @param hbaseDataItemBase
     * @return
     */
    public boolean get(String tableName, HbaseModelBase hbaseDataItemBase) throws Exception {
        HTable table = (HTable) this.tablePool.getTable(tableName);

        Get get = hbaseDataItemBase.getGet();

        try {
            Result rawRes = table.get(get);
            if (rawRes == null || rawRes.isEmpty()) {
                return false;
            }
            else {
                hbaseDataItemBase.initFromResult(rawRes);
                return true;
            }
        } catch (Exception e) {
            throw e;
        } finally {
            table.close();
        }
    }

    /**
     * 从hbase中获取多条数据，结果会填充在原来的数据结构里面
     * @param tableName
     * @param hbaseDataItemBaseList
     * @return
     * @throws Exception
     */
    public boolean multiGet(String tableName, List<HbaseModelBase> hbaseDataItemBaseList) throws Exception {
        HTable table = (HTable) this.tablePool.getTable(tableName);

        List<Get> gets = new ArrayList<>();
        for(HbaseModelBase item : hbaseDataItemBaseList) {
            Get get = item.getGet();
            gets.add(get);
        }

        boolean flag = true;
        try {
            int index = 0;
            Result[] rawRes = table.get(gets);
            for(Result item : rawRes) {
                HbaseModelBase data = hbaseDataItemBaseList.get(index++);
                if (item.isEmpty()) {
                    flag = false;
                } else {
                    data.initFromResult(item);
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            table.close();
        }

        return flag;
    }


    /**
     * 从数据库中扫描一批数据，数据扫描范围由startRow和endRow确定
     * @param tableName
     * @param startRow
     * @param endRow
     * @return
     * @throws Exception
     */
    public List<HbaseModelBase> scan(String tableName, HbaseModelBase startRow,
                                        HbaseModelBase endRow, FilterList filterList) throws Exception {
        HTable table = (HTable) this.tablePool.getTable(tableName);

        List<HbaseModelBase> resultList = new ArrayList<>();
        Scan scan = this.getScanObject(startRow, endRow, filterList);

        try {
            ResultScanner rs = table.getScanner(scan);
            Class clazz = startRow.getClass();
            for (Result r : rs) {
                HbaseModelBase item = (HbaseModelBase) clazz.newInstance();
                resultList.add(item.initFromResult(r));
            }
        } catch (Exception e) {
            throw e;
        } finally {
            table.close();
        }

        return resultList;
    }

    /**
     * 从数据库中扫描一批数据，数据扫描范围由startRow和endRow确定，对于数据量比较大的场景，返回一个迭代器，分批次获取数据
     * @param tableName
     * @param startRow
     * @param endRow
     * @return
     * @throws Exception
     */
    public ScanResultIterator scanWithIterator(String tableName, HbaseModelBase startRow,
                                   HbaseModelBase endRow, FilterList filterList) throws Exception {
        HTable table = (HTable) this.tablePool.getTable(tableName);

        ScanResultIterator iterator = null;
        Scan scan = this.getScanObject(startRow, endRow, filterList);

        try {
            Class clazz = startRow.getClass();
            ResultScanner rs = table.getScanner(scan);
            iterator = new ScanResultIterator(clazz, rs);
        } catch (Exception e) {
            throw e;
        } finally {
            table.close();
        }

        return iterator;
    }

    private Scan getScanObject(HbaseModelBase startObj,
                               HbaseModelBase endObj,
                               FilterList filterList) throws Exception {
        Scan scan = new Scan();
        byte [] start = startObj.getRowKey();
        byte [] stop = endObj.getRowKey();
        scan.setStartRow(start);
        scan.setStopRow(stop);

        String familyName = startObj.getFamilyName();
        byte [] family = Bytes.toBytes(familyName);

        for(String field : startObj.getDataFieldNameMappings().values()) {
            scan.addColumn(family, Bytes.toBytes(field));
        }

        if(filterList != null) {
            scan.setFilter(filterList);
        }
        return scan;
    }

    public static class ScanResultIterator {
        private Class clazz;
        private ResultScanner scanner;

        public ScanResultIterator(Class clazz, ResultScanner scanner) {
            this.clazz = clazz;
            this.scanner = scanner;
        }

        public synchronized HbaseModelBase next() throws Exception {
            Result result = this.scanner.next();

            if (result != null) {
                HbaseModelBase item = (HbaseModelBase) this.clazz.newInstance();
                item.initFromResult(result);

                return item;
            }
            else {
                return null;
            }
        }

        public synchronized List<HbaseModelBase> next(int size) throws Exception {
            List<HbaseModelBase> result = new LinkedList<>();

            for (int i = 0; i < size; i++) {
                HbaseModelBase item = this.next();

                if (item != null) {
                    result.add(this.next());
                } else {
                    break;
                }
            }

            return result;
        }
    }
}
