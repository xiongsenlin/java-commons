package org.xsl.common.db;

import com.alibaba.fastjson.annotation.JSONField;
import org.xsl.common.annotation.MysqlFieldAnnotation;
import org.xsl.common.json.JsonHelper;
import org.xsl.common.string.StringUtils;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by xiongsenlin on 17/1/11.
 */
public abstract class MysqlModelBase {
    @JSONField(serialize = false, deserialize = false)
    protected String tableName;

    @JSONField(serialize = false, deserialize = false)
    protected Class<?> clazz = this.getClass();

    @JSONField(serialize = false, deserialize = false)
    protected static final String INSERT_SQL_FORMAT = "replace into {0} ({1}) values({2})";

    @JSONField(serialize = false, deserialize = false)
    protected static final String DELETE_SQL_FORMAT = "delete from {0} where {1}";

    @JSONField(serialize = false, deserialize = false)
    protected static final String UPDATE_SQL_FORMAT = "update {0} set {1} where {2}";

    /**
     * 表示支持的数据类型
     */
    @JSONField(serialize = false, deserialize = false)
    protected static List<? extends Class<? extends Serializable>> SUPPORT_DATA_TYPES
            = Arrays.asList(Integer.class, Long.class, Date.class, Float.class, Double.class, String.class);

    /**
     * 保存字段的属性配置，用于将数据库中的数据转换成bean或者将bean转换成sql语句
     */
    @JSONField(serialize = false, deserialize = false)
    private static final Map<Class<?>, Map<String, FieldAttr>> FIELD_ATTR_MAP = new HashMap<>();

    public MysqlModelBase(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new RuntimeException("Table name can not be empty");
        }
        this.tableName = tableName;

        if (!FIELD_ATTR_MAP.containsKey(this.clazz)) {
            synchronized (MysqlModelBase.class) {
                if (!FIELD_ATTR_MAP.containsKey(this.clazz)) {
                    this.initFieldAttr();
                }
            }
        }
    }

    @JSONField(serialize = false, deserialize = false)
    public String getInsertSql() throws Exception {
        StringBuilder fieldsBuilder = new StringBuilder();
        StringBuilder valuesBuilder = new StringBuilder();

        Map<String, FieldAttr> fieldAttrMap = FIELD_ATTR_MAP.get(this.clazz);

        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();

            if (fieldAttrMap.containsKey(fieldName)) {
                FieldAttr fieldAttr = fieldAttrMap.get(fieldName);
                if (!fieldAttr.toDB) {
                    continue;
                }

                field.setAccessible(true);
                Object value = field.get(this);
                if (value == null) {
                    if (fieldAttr.primaryKey) {
                        throw new IllegalArgumentException("Primary key [" + fieldName + "] can not be null");
                    }
                    if (!fieldAttr.nullable) {
                        throw new IllegalArgumentException("Field [" + fieldName + "] can not be null");
                    }
                }

                if (fieldsBuilder.length() > 0) {
                    fieldsBuilder.append(", ");
                    valuesBuilder.append(", ");
                }

                fieldsBuilder.append("`" + fieldAttr.dbFieldName + "`");
                valuesBuilder.append(this.getValue(value, fieldAttr));
            }
        }

        return MessageFormat.format(INSERT_SQL_FORMAT,
                this.tableName, fieldsBuilder.toString(), valuesBuilder.toString());
    }

    @JSONField(serialize = false, deserialize = false)
    public String getDeleteSql() throws Exception {
        StringBuilder whereClauseBuilder = new StringBuilder();

        Map<String, FieldAttr> fieldAttrMap = FIELD_ATTR_MAP.get(this.clazz);
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();

            if (fieldAttrMap.containsKey(fieldName)) {
                FieldAttr fieldAttr = fieldAttrMap.get(fieldName);
                if (!fieldAttr.primaryKey) {
                    continue;
                }

                field.setAccessible(true);
                Object value = field.get(this);
                if (value == null) {
                    throw new IllegalArgumentException("Primary key [" + fieldName + "] can not be null");
                }

                if (whereClauseBuilder.length() > 0) {
                    whereClauseBuilder.append(" and ");
                }

                whereClauseBuilder.append(fieldAttr.dbFieldName + " = " + this.getValue(value, fieldAttr));
            }
        }

        if (whereClauseBuilder.length() == 0) {
            throw new RuntimeException("No primary key defined");
        }

        return MessageFormat.format(DELETE_SQL_FORMAT, this.tableName, whereClauseBuilder.toString());
    }

    @JSONField(serialize = false, deserialize = false)
    public String getUpdateSql() throws Exception {
        StringBuilder setClauseBuilder = new StringBuilder();
        StringBuilder whereClauseBuilder = new StringBuilder();

        Map<String, FieldAttr> fieldAttrMap = FIELD_ATTR_MAP.get(this.clazz);
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();

            if (fieldAttrMap.containsKey(fieldName)) {
                FieldAttr fieldAttr = fieldAttrMap.get(fieldName);

                field.setAccessible(true);
                Object value = field.get(this);
                if (value == null && fieldAttr.primaryKey) {
                    throw new IllegalArgumentException("Primary key [" + fieldName + "] can not be null");
                }

                if (fieldAttr.primaryKey) {
                    if (whereClauseBuilder.length() > 0) {
                        whereClauseBuilder.append(" and ");
                    }
                    whereClauseBuilder.append(fieldAttr.dbFieldName + " = " + this.getValue(value, fieldAttr));
                }
                else {
                    if (setClauseBuilder.length() > 0) {
                        setClauseBuilder.append(", ");
                    }
                    setClauseBuilder.append(fieldAttr.dbFieldName + "=" + this.getValue(value, fieldAttr));
                }
            }
        }

        return MessageFormat.format(UPDATE_SQL_FORMAT,
                this.tableName, setClauseBuilder.toString(), whereClauseBuilder.toString());
    }

    public MysqlModelBase initFromMap(Map<String, String> data) throws Exception {
        if (data == null || data.isEmpty()) {
            return this;
        }

        Map<String, FieldAttr> fieldAttrMap = FIELD_ATTR_MAP.get(this.clazz);

        Field[] fields = this.clazz.getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            Class type = field.getType();

            if (fieldAttrMap.containsKey(fieldName)) {
                FieldAttr fieldAttr = fieldAttrMap.get(fieldName);

                String orgValue = data.get(fieldAttr.dbFieldName);
                if (orgValue == null || fieldAttr.fromDB == false) {
                    continue;
                }

                Object value;
                if (type == Integer.class) {
                    value = Integer.parseInt(orgValue);
                } else if (type == Long.class) {
                    value = Long.parseLong(orgValue);
                } else if (type == Float.class) {
                    value = Float.parseFloat(orgValue);
                } else if (type == Double.class) {
                    value = Double.parseDouble(orgValue);
                } else if (type == Date.class) {
                    SimpleDateFormat sdf = new SimpleDateFormat(fieldAttr.format);
                    value = sdf.parse(orgValue);
                } else {
                    value = orgValue;
                }

                field.setAccessible(true);
                field.set(this, value);
            }

        }

        return this;
    }

    @Override
    public String toString() {
        return JsonHelper.toJson(this);
    }

    private String getValue(Object value, FieldAttr fieldAttr) {
        if (value == null) {
            return "null";
        }

        if (fieldAttr.fieldType == Date.class) {
            SimpleDateFormat sdf = new SimpleDateFormat(fieldAttr.format);
            return "'" + sdf.format((Date) value) + "'";
        } else if (fieldAttr.fieldType == String.class) {
            return "'" + value + "'";
        } else {
            return value + "";
        }
    }

    private void initFieldAttr() {
        Map<String, FieldAttr> fieldAttrMap = new HashMap<>();
        FIELD_ATTR_MAP.put(this.clazz, fieldAttrMap);

        boolean primaryKeyDefined = false;
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            Annotation[] annotations = field.getDeclaredAnnotations();
            if (annotations != null) {
                for (Annotation annotation : annotations) {
                    if (annotation instanceof MysqlFieldAnnotation) {
                        MysqlFieldAnnotation mysqlField = (MysqlFieldAnnotation) annotation;

                        Class type = field.getType();
                        if (!SUPPORT_DATA_TYPES.contains(type)) {
                            throw new RuntimeException("Field type [" + type.getName() + "] is not supported yet");
                        }

                        String fieldName = field.getName();
                        fieldAttrMap.put(fieldName, new FieldAttr(mysqlField.nullable(),
                                mysqlField.primaryKey(), mysqlField.dbFieldName(),
                                mysqlField.format(), mysqlField.fromDB(), mysqlField.toDB(), fieldName, type));

                        if (mysqlField.primaryKey()) {
                            primaryKeyDefined = true;
                        }

                        break;
                    }
                }
            }
        }

        if (!primaryKeyDefined) {
            throw new RuntimeException("No primary key defined for the table");
        }
    }

    private static class FieldAttr {
        public boolean nullable;
        public boolean primaryKey;
        public String dbFieldName;
        public String format;
        public boolean fromDB;
        public boolean toDB;

        public String fieldName;
        public Class<?> fieldType;

        public FieldAttr() {}

        public FieldAttr(boolean nullable, boolean primaryKey, String dbFieldName,
                         String format, boolean fromDB, boolean toDB, String fieldName, Class<?> fieldType) {
            this.nullable = nullable;
            this.primaryKey = primaryKey;
            this.dbFieldName = dbFieldName;
            this.format = format;
            this.fromDB = fromDB;
            this.toDB = toDB;

            this.fieldName = fieldName;
            this.fieldType = fieldType;

            if (StringUtils.isEmpty(this.dbFieldName)) {
                this.dbFieldName = fieldName;
            }
        }
    }
}
