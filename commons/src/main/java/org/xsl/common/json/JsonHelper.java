package org.xsl.common.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * reference: <a href="https://github.com/alibaba/fastjson"></a>
 *
 * Created by xiongsenlin on 16/1/6.
 */
public class JsonHelper {
    private static final Type DEFAULT_TYPE = new TypeReference<Map<String, Object>>() {}.getType();

    private Map<String, Object> dataMap;

    public JsonHelper(String json) {
        this.dataMap = JSON.parseObject(json, DEFAULT_TYPE);
    }

    public Object getObjectValue(String key) {
        return this.getValue(key);
    }

    public String getStringValue(String key) {
        Object data = this.getValue(key);
        if (data == null) {
            return null;
        }

        if (data instanceof String) {
            return (String) data;
        } else {
            return data.toString();
        }
    }

    public Integer getIntValue(String key) {
        Object data = this.getValue(key);
        if (data == null) {
            return null;
        }

        if (data instanceof Integer) {
            return (Integer) data;
        } else {

            return Integer.parseInt(data.toString());
        }
    }

    public Long getLongValue(String key) {
        Object data = this.getValue(key);
        if (data == null) {
            return null;
        }

        if (data instanceof Long) {
            return (Long) data;
        } else {
            return Long.parseLong(data.toString());
        }
    }

    public Double getDoubleValue(String key) {
        Object data = this.getValue(key);
        if (data == null) {
            return null;
        }

        if (data instanceof Double) {
            return (Double) data;
        } else {
            return Double.parseDouble(data.toString());
        }
    }

    public Float getFloatValue(String key) {
        Object data = this.getValue(key);
        if (data == null) {
            return null;
        }

        if (data instanceof Double) {
            return (Float) data;
        } else {
            return Float.parseFloat(data.toString());
        }
    }

    public Boolean getBooleanValue(String key) {
        Object data = this.getValue(key);
        if (data == null) {
            return null;
        }

        if (data instanceof Boolean) {
            return (Boolean) data;
        } else {
            return Boolean.parseBoolean(data.toString());
        }
    }

    public Object getValue(String key) {
        if (key == null || key.isEmpty()) {
            return null;
        }

        Object result = this.dataMap;
        String [] parts = key.split("\\.");
        for (String part : parts) {
            if (result == null) {
                return null;
            }

            if (result instanceof Map) {
                result = ((Map) result).get(part.trim());
            } else {
                return null;
            }
        }

        return result;
    }

    public static <T> String toJson(T data) {
        return JSON.toJSONString(data);
    }

    public static <T> T toObject(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }
}
