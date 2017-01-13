package org.xsl.common.db.hbase;

import org.xsl.common.annotation.HbaseFieldAnnotation;
import com.alibaba.fastjson.annotation.JSONField;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.xsl.common.json.JsonHelper;
import org.xsl.common.util.BytesUtil;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by xiongsenlin on 15/7/8.
 */
public abstract class HbaseModelBase {
    @JSONField(serialize = false, deserialize = false)
    protected Class<?> clazz = this.getClass();

    /**
     * 目前只支持以下几种数据类型
     */
    @JSONField(serialize = false, deserialize = false)
    protected static List<? extends Class<? extends Serializable>>
            SUPPORT_TYPES = Arrays.asList(Integer.class, Long.class, Float.class, Double.class, String.class);

    /**
     * rowKey可以设置一个公共的前缀
     */
    @JSONField(serialize = false, deserialize = false)
    protected String rowKeyPrefix = "";

    /**
     * 列族名称，默认为cf，如果不为默认值，则在子类中需要对该字段重新赋值
     */
    @JSONField(serialize = false, deserialize = false)
    protected String familyName = "cf";

    /**
     * rowKey之间的分隔符号，默认为下划线，如果不为默认值，则在子类中重新赋值
     */
    @JSONField(serialize = false, deserialize = false)
    protected String rowKeySeparator = "_";

    @JSONField(serialize = false, deserialize = false)
    protected static final Map<Class<?>, Map<String, Integer>> ROW_KEY_INDEX_MAP = new HashMap<>();

    @JSONField(serialize = false, deserialize = false)
    protected static final Map<Class<?>, Map<String, String>> FIELD_NAME_MAPPINGS = new HashMap<>();

    public HbaseModelBase() {
        if (!FIELD_NAME_MAPPINGS.containsKey(this.clazz)) {
            synchronized (HbaseModelBase.class) {
                if (!FIELD_NAME_MAPPINGS.containsKey(this.clazz)) {
                    this.initFieldAttr();
                }
            }
        }
    }

    /**
     * 返回当前对象的rowKey, rowKey由{@link HbaseFieldAnnotation}{@link HbaseFieldAnnotation#isRowKeyField()}
     * {@code true}的所有字段组成，其顺序为类中字段定义的顺序，由{@link #rowKeySeparator}拼接而成
     * @return
     */
    @JSONField(serialize = false, deserialize = false)
    public byte[] getRowKey() throws Exception {
        StringBuilder sb = new StringBuilder();

        if (!this.rowKeyPrefix.isEmpty()) {
            sb.append(this.rowKeyPrefix);
            sb.append(this.rowKeySeparator);
        }

        Map<String, Integer> rowKeyIndexMap = ROW_KEY_INDEX_MAP.get(this.clazz);
        Object [] rowKeys = new Object[rowKeyIndexMap.size()];

        for (Map.Entry<String, Integer> entry : rowKeyIndexMap.entrySet()) {
            String fieldName = entry.getKey();
            int index = entry.getValue();

            Field field = this.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            Object value = field.get(this);
            rowKeys[index] = value;
        }

        for (int i = 0; i < rowKeyIndexMap.size(); i++) {
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
    @JSONField(serialize = false, deserialize = false)
    public Put getPut() throws Exception {
        Put put = new Put(this.getRowKey());

        Map<String, String> dataFieldNameMappings = FIELD_NAME_MAPPINGS.get(this.clazz);

        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            if (dataFieldNameMappings.containsKey(fieldName)) {
                String qualifier = dataFieldNameMappings.get(fieldName);

                field.setAccessible(true);
                Object value = field.get(this);
                if (value != null) {
                    if (value instanceof byte[]) {
                        put.add(Bytes.toBytes(this.familyName), Bytes.toBytes(qualifier), (byte[]) value);
                    }
                    else {
                        put.add(Bytes.toBytes(this.familyName),
                                Bytes.toBytes(qualifier), Bytes.toBytes(value.toString()));
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
    @JSONField(serialize = false, deserialize = false)
    public Delete getDelete() throws Exception {
        Delete delete = new Delete(this.getRowKey());
        return delete;
    }

    /**
     * 获取当前对象的Get对象，以便从hbase中查询数据
     * @return
     */
    @JSONField(serialize = false, deserialize = false)
    public Get getGet() throws Exception {
        Get get = new Get(this.getRowKey());
        return get;
    }

    /**
     * 从Result中获取实体对象
     * @param result
     * @return
     */
    public HbaseModelBase initFromResult(Result result) throws Exception {
        if (result == null) {
            return this;
        }

        String rowStr = Bytes.toString(result.getRow());
        if (rowStr == null) {
            return this;
        }

        Map<String, Integer> rowKeyIndexMap = ROW_KEY_INDEX_MAP.get(this.clazz);
        Map<String, String> dataFieldNameMappings = FIELD_NAME_MAPPINGS.get(this.clazz);

        if (this.rowKeyPrefix != "") {
            rowStr = rowStr.substring(rowStr.indexOf(this.rowKeySeparator) + 1);
        }

        String[] rowItems = rowStr.split(this.rowKeySeparator);
        if (rowItems.length != rowKeyIndexMap.size()) {
            throw new RuntimeException("Invalid Result object, the row field does not match");
        }

        Field[] fields = this.getClass().getDeclaredFields();
        byte[] familyBytes = Bytes.toBytes(this.familyName);

        for (Field field : fields) {
            String fieldName = field.getName();
            Class type = field.getType();

            if (dataFieldNameMappings.containsKey(fieldName)) {
                String hbaseFieldName = dataFieldNameMappings.get(fieldName);
                byte[] rawValue = result.getValue(familyBytes, Bytes.toBytes(hbaseFieldName));
                field.setAccessible(true);
                field.set(this, this.getValue(rawValue, type));
            }
            if (rowKeyIndexMap.containsKey(fieldName)) {
                int index = rowKeyIndexMap.get(fieldName);

                field.setAccessible(true);
                field.set(this, this.getValue(rowItems[index], type));
            }
        }

        return this;
    }

    @JSONField(serialize = false, deserialize = false)
    private Object getValue(byte[] rawValue, Class type) {
        if (type == Integer.class) {
            return BytesUtil.getIntegerFromBytes(rawValue);
        } else if (type == Long.class) {
            return BytesUtil.getLongFromBytes(rawValue);
        } else if (type == Float.class) {
            return BytesUtil.getFloatFromBytes(rawValue);
        } else if (type == Double.class) {
            return BytesUtil.getDoubleFromBytes(rawValue);
        } else {
            return Bytes.toString(rawValue);
        }
    }

    @JSONField(serialize = false, deserialize = false)
    private Object getValue(String value, Class type) {
        if (type == Integer.class) {
            return Integer.parseInt(value);
        } else if (type == Long.class) {
            return Long.parseLong(value);
        } else if (type == Float.class) {
            return Float.parseFloat(value);
        } else if (type == Double.class) {
            return Double.parseDouble(value);
        } else {
            return value;
        }
    }

    @JSONField(serialize = false, deserialize = false)
    public String getFamilyName() {
        return familyName;
    }

    @JSONField(serialize = false, deserialize = false)
    public Map<String, String> getDataFieldNameMappings() {
        return FIELD_NAME_MAPPINGS.get(this.clazz);
    }

    @Override
    public String toString() {
        return JsonHelper.toJson(this);
    }

    private void initFieldAttr() {
        Map<String, Integer> rowKeyIndexMapping = new HashMap<>();
        ROW_KEY_INDEX_MAP.put(this.clazz, rowKeyIndexMapping);

        Map<String, String> fieldNameMapping = new HashMap<>();
        FIELD_NAME_MAPPINGS.put(this.clazz, fieldNameMapping);

        Field[] fields = this.getClass().getDeclaredFields();
        List<Integer> rowKeyIndexes = new ArrayList<Integer>();

        for (Field field : fields) {
            Annotation[] annotations = field.getDeclaredAnnotations();
            if (annotations != null) {
                for (Annotation annotation : annotations) {
                    if (annotation instanceof HbaseFieldAnnotation) {
                        HbaseFieldAnnotation hbaseField = (HbaseFieldAnnotation) annotation;

                        Class type = field.getType();
                        if (!SUPPORT_TYPES.contains(type)) {
                            throw new RuntimeException("Field type [" + type.getName() + "] is not supported yet");
                        }

                        String fieldName = field.getName();
                        String mappingFiledName = hbaseField.mappingFiledName();

                        boolean isRowKey = hbaseField.isRowKeyField();
                        boolean isDataField = hbaseField.isDataField();
                        int rowKeyIndex = hbaseField.rowKeyIndex();

                        if (isRowKey) {
                            if (rowKeyIndex == -1) {
                                throw new RuntimeException("Row key index is not valid");
                            }
                            rowKeyIndexes.add(rowKeyIndex);
                            rowKeyIndexMapping.put(fieldName, rowKeyIndex);
                        }
                        if (isDataField) {
                            mappingFiledName = (mappingFiledName.equals("") ? fieldName : mappingFiledName);
                            fieldNameMapping.put(fieldName, mappingFiledName);
                        }
                        break;
                    }
                }
            }
        }
        if (rowKeyIndexMapping.size() == 0 || fieldNameMapping.size() == 0) {
            throw new RuntimeException("Field config error, no row key field or data field");
        }

        Collections.sort(rowKeyIndexes);
        for (int i = 0; i < rowKeyIndexes.size(); i++) {
            if (rowKeyIndexes.get(i) != i) {
                throw new RuntimeException("Row key index is not valid");
            }
        }
    }
}
