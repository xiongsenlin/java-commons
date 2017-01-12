package org.xsl.common.base;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.xsl.common.annotation.HbaseFieldAnnotation;
import org.xsl.common.db.ConnectionPool;
import org.xsl.common.util.BytesUtil;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by xiongsenlin on 15/7/8.
 */
public abstract class HbaseDataItemBase {
    private static Logger logger = Logger.getLogger(ConnectionPool.class);

    /**
     * 目前只支持以下几种数据类型
     */
    protected static List<? extends Class<? extends Serializable>>
            supportedTypes = Arrays.asList(Integer.class, Long.class, Float.class, Double.class, String.class);

    /**
     * rowKey可以设置一个公共的前缀
     */
    protected String rowKeyPrefix = "";
    /**
     * 列族名称，默认为cf，如果不为默认值，则在子类中需要对该字段重新赋值
     */
    protected String familyName = "cf";
    /**
     * rowKey之间的分隔符号，默认为下划线，如果不为默认值，则在子类中重新赋值
     */
    protected String rowKeySeparator = "_";

    protected Map<String, Integer> rowKeyIndexMap;
    protected Map<String, String> dataFieldNameMappings;

    public HbaseDataItemBase() {
        this.rowKeyIndexMap = new HashMap<>();
        this.dataFieldNameMappings = new HashMap<>();
        this.init();
    }

    /**
     * 返回当前对象的rowKey, rowKey由HbaseFieldAnnotation注解中字段isRowKeyField为
     * true的所有字段组成，其顺序为类中字段定义的顺序，由rowKeySeparator拼接而成
     * @return
     */
    public byte[] getRowKey() throws Exception {
        StringBuilder sb = new StringBuilder();

        if (!this.rowKeyPrefix.isEmpty()) {
            sb.append(this.rowKeyPrefix);
            sb.append(this.rowKeySeparator);
        }

        Object [] rowKeys = new Object[this.rowKeyIndexMap.size()];

        for (Map.Entry<String, Integer> entry : this.rowKeyIndexMap.entrySet()) {
            String fieldName = entry.getKey();
            int index = entry.getValue();

            Field field = this.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            Object value = field.get(this);
            rowKeys[index] = value;
        }

        for (int i = 0; i < this.rowKeyIndexMap.size(); i++) {
            if (rowKeys[i] != null) {
                sb.append(rowKeys[i].toString());
            }
            sb.append(this.rowKeySeparator);
        }

        String rowKey = sb.toString();
        rowKey = rowKey.substring(0, rowKey.length() - this.rowKeySeparator.length());

        return Bytes.toBytes(rowKey);
    }

    /**
     * 获取当前对象对应的hbase中的put对象，以便插入hbase
     * @return
     */
    public Put getPut() throws Exception {
        Put put = new Put(this.getRowKey());

        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            if (this.dataFieldNameMappings.containsKey(fieldName)) {
                String qualifier = this.dataFieldNameMappings.get(fieldName);

                field.setAccessible(true);
                Object value = field.get(this);
                if (value != null) {
                    if (value instanceof byte[]) {
                        put.add(Bytes.toBytes(this.familyName), Bytes.toBytes(qualifier), (byte[]) value);
                    } else {
                        put.add(Bytes.toBytes(this.familyName), Bytes.toBytes(qualifier), Bytes.toBytes(value.toString()));
                    }
                }
            }
        }

        return put;
    }

    /**
     * 获取当前对象的Delete对象，以便从hbase中删除该数据
     * @return
     */
    public Delete getDelete() throws Exception {
        Delete delete = new Delete(this.getRowKey());
        return delete;
    }

    /**
     * 获取当前对象的Get对象，以便从hbase中查询数据
     * @return
     */
    public Get getGet() throws Exception {
        Get get = new Get(this.getRowKey());
        return get;
    }

    /**
     * 从Result中获取实体对象
     * @param result
     * @return
     */
    public HbaseDataItemBase initFromResult(Result result) {
        if (result == null) {
            return this;
        }

        String rowStr = Bytes.toString(result.getRow());
        if (rowStr == null) {
            return this;
        }

        if (this.rowKeyPrefix != "") {
            rowStr = rowStr.substring(rowStr.indexOf(this.rowKeySeparator) + 1);
        }

        String[] rowItems = rowStr.split(this.rowKeySeparator);

        if (rowItems.length != this.rowKeyIndexMap.size()) {
            throw new RuntimeException("Invalid Result object, the row field does not match");
        }

        Field[] fields = this.getClass().getDeclaredFields();
        byte[] familyBytes = Bytes.toBytes(this.familyName);

        for (Field field : fields) {
            String fieldName = field.getName();
            Class type = field.getType();

            try {
                Object value;
                if (this.dataFieldNameMappings.containsKey(fieldName)) {
                    String hbaseFieldName = this.dataFieldNameMappings.get(fieldName);
                    byte[] rawValue = result.getValue(familyBytes, Bytes.toBytes(hbaseFieldName));

                    if (type == Integer.class) {
                        value = BytesUtil.getIntegerFromBytes(rawValue);
                    } else if (type == Long.class) {
                        value = BytesUtil.getLongFromBytes(rawValue);
                    } else if (type == Float.class) {
                        value = BytesUtil.getFloatFromBytes(rawValue);
                    } else if (type == Double.class) {
                        value = BytesUtil.getDoubleFromBytes(rawValue);
                    } else {
                        value = Bytes.toString(rawValue);
                    }
                    try {
                        field.setAccessible(true);
                        field.set(this, value);
                    } catch (IllegalAccessException e) {
                        logger.error("Get field value by reflection error", e);
                    }
                }
                if (this.rowKeyIndexMap.containsKey(fieldName)) {
                    int index = this.rowKeyIndexMap.get(fieldName);
                    if (type == Integer.class) {
                        value = Integer.parseInt(rowItems[index]);
                    } else if (type == Long.class) {
                        value = Long.parseLong(rowItems[index]);
                    } else if (type == Float.class) {
                        value = Float.parseFloat(rowItems[index]);
                    } else if (type == Double.class) {
                        value = Double.parseDouble(rowItems[index]);
                    } else {
                        value = rowItems[index];
                    }
                    try {
                        field.setAccessible(true);
                        field.set(this, value);
                    } catch (IllegalAccessException e) {
                        logger.error("Get field value by reflection error", e);
                    }
                }
            } catch (Exception e) {
                logger.error("Set field value by Result object error", e);
            }
        }
        return this;
    }

    private void init() {
        Field[] fields = this.getClass().getDeclaredFields();
        List<Integer> rowKeyIndexes = new ArrayList<Integer>();

        for (Field field : fields) {
            Annotation[] annotations = field.getDeclaredAnnotations();
            if (annotations != null) {
                for (Annotation annotation : annotations) {
                    if (annotation instanceof HbaseFieldAnnotation) {
                        HbaseFieldAnnotation hbaseField = (HbaseFieldAnnotation) annotation;

                        Class type = field.getType();
                        if (!this.supportedTypes.contains(type)) {
                            throw new RuntimeException("Field type is not supported yet");
                        }

                        String fieldName = field.getName();
                        String mappingFiledName = hbaseField.mappingFiledName();

                        boolean isRowKey = hbaseField.isRowKeyField();
                        boolean isDataField = hbaseField.isDataField();
                        int rowKeyIndex = hbaseField.rowKeyIndex();

                        if (isRowKey) {
                            if (rowKeyIndex == -1) {
                                throw new RuntimeException("RowKeyIndex config error");
                            }
                            rowKeyIndexes.add(rowKeyIndex);
                            this.rowKeyIndexMap.put(fieldName, rowKeyIndex);
                        }
                        if (isDataField) {
                            mappingFiledName = (mappingFiledName.equals("") ? fieldName : mappingFiledName);
                            this.dataFieldNameMappings.put(fieldName, mappingFiledName);
                        }
                        break;
                    }
                }
            }
        }
        if (this.rowKeyIndexMap.size() == 0 || this.dataFieldNameMappings.size() == 0) {
            throw new RuntimeException("Field config error, no rowKey field or data field");
        }

        Collections.sort(rowKeyIndexes);
        for (int i = 0; i < rowKeyIndexes.size(); i++) {
            if (rowKeyIndexes.get(i) != i) {
                throw new RuntimeException("RowKeyIndex config error");
            }
        }
    }

    public String getFamilyName() {
        return familyName;
    }

    public Map<String, String> getDataFieldNameMappings() {
        return dataFieldNameMappings;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            if (this.rowKeyIndexMap.containsKey(name)
                    || this.dataFieldNameMappings.containsKey(name)) {
                field.setAccessible(true);
                try {
                    Object value = field.get(this);
                    sb.append(" " + name + ": " + value);
                } catch (IllegalAccessException e) {
                    logger.error("Get field value error", e);
                }
            }
        }
        return sb.toString();
    }
}
