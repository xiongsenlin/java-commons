package org.xsl.common.db.mysql;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by xiongsenlin on 17/1/11.
 */
public abstract class MysqlCrudBase <T extends MysqlModelBase> {
    private Class<T> clazz;
    private MysqlHelper mysqlHelper;

    public MysqlCrudBase(MysqlHelper mysqlHelper) {
        this.mysqlHelper = mysqlHelper;

        Type superClass = this.getClass().getGenericSuperclass();
        if (!(superClass instanceof ParameterizedType)) {
            throw new RuntimeException("Raw type found, please use generic type");
        }

        this.clazz = (Class<T>) ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    public void insert(T data) throws Exception {
        this.mysqlHelper.update(data.getInsertSql());
    }

    public void insert(String sql) throws Exception {
        this.mysqlHelper.update(sql);
    }

    public void delete(T data) throws Exception {
        this.mysqlHelper.update(data.getDeleteSql());
    }

    public void delete(String sql) throws Exception {
        this.mysqlHelper.update(sql);
    }

    public void update(T data) throws Exception {
        this.mysqlHelper.update(data.getUpdateSql());
    }

    public void update(String sql) throws Exception {
        this.mysqlHelper.update(sql);
    }

    public List<T> query(String sql) throws Exception {
        List<Map<String, String>> orgData = this.mysqlHelper.query(sql);

        List<T> result = new LinkedList<>();
        for (Map<String, String> item : orgData) {
            T tmpData = this.clazz.newInstance();
            result.add((T) tmpData.initFromMap(item));
        }

        return result;
    }
}
