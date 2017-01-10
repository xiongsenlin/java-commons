package org.xsl.common.test.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import org.junit.Test;
import org.xsl.common.json.JsonHelper;
import java.lang.Object;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by xiongsenlin on 17/1/6.
 */
public class TestJsonHelper {
    private JsonHelper jsonHelper;

    public TestJsonHelper() {
    }

    @Test
    public void testSingleLevel() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("int", 1);
        map.put("int_str", "2");

        map.put("string", "string");
        map.put("string_int", 10);

        map.put("long", 1);
        map.put("long_str", "2");

        map.put("float", 1.0);
        map.put("float_str", "2.0");

        map.put("double", 1.0);
        map.put("double_str", "2.0");

        map.put("boolean", true);
        map.put("boolean_str", "true");

        this.jsonHelper = new JsonHelper(JSON.toJSONString(map));

        String str = this.jsonHelper.getStringValue("string");
        assertEquals(str, "string");
        str = this.jsonHelper.getStringValue("string_int");
        assertEquals(str, "10");

        Integer integer = this.jsonHelper.getIntValue("int");
        assertTrue(integer == 1);
        integer = this.jsonHelper.getIntValue("int_str");
        assertTrue(integer == 2);

        Long ll = this.jsonHelper.getLongValue("long");
        assertTrue(ll == 1);
        ll = this.jsonHelper.getLongValue("long_str");
        assertTrue(ll == 2);

        Double dd = this.jsonHelper.getDoubleValue("double");
        assertTrue(dd == 1.0);
        dd = this.jsonHelper.getDoubleValue("double_str");
        assertTrue(dd == 2.0);

        Float ff = this.jsonHelper.getFloatValue("float");
        assertTrue(ff == 1.0);
        ff = this.jsonHelper.getFloatValue("float_str");
        assertTrue(ff == 2.0);

        Boolean bb = this.jsonHelper.getBooleanValue("boolean");
        assertTrue(bb);
        bb = this.jsonHelper.getBooleanValue("boolean_str");
        assertTrue(bb);
    }

    @Test
    public void testMultiLevel() {
        Map<String, Object> inner = new HashMap<String, Object>();
        inner.put("int", 1);
        inner.put("int_str", "2");

        inner.put("string", "string");
        inner.put("string_int", 10);

        inner.put("long", 1);
        inner.put("long_str", "2");

        inner.put("float", 1.0);
        inner.put("float_str", "2.0");

        inner.put("double", 1.0);
        inner.put("double_str", "2.0");

        inner.put("boolean", true);
        inner.put("boolean_str", "true");

        Map<String, Object> outer = new HashMap<String, Object>();
        outer.put("fields", inner);

        this.jsonHelper = new JsonHelper(JSON.toJSONString(outer));

        String str = this.jsonHelper.getStringValue("fields.string");
        assertEquals(str, "string");
        str = this.jsonHelper.getStringValue("fields.string_int");
        assertEquals(str, "10");

        Integer integer = this.jsonHelper.getIntValue("fields.int");
        assertTrue(integer == 1);
        integer = this.jsonHelper.getIntValue("fields.int_str");
        assertTrue(integer == 2);

        Long ll = this.jsonHelper.getLongValue("fields.long");
        assertTrue(ll == 1);
        ll = this.jsonHelper.getLongValue("fields.long_str");
        assertTrue(ll == 2);

        Double dd = this.jsonHelper.getDoubleValue("fields.double");
        assertTrue(dd == 1.0);
        dd = this.jsonHelper.getDoubleValue("fields.double_str");
        assertTrue(dd == 2.0);

        Float ff = this.jsonHelper.getFloatValue("fields.float");
        assertTrue(ff == 1.0);
        ff = this.jsonHelper.getFloatValue("fields.float_str");
        assertTrue(ff == 2.0);

        Boolean bb = this.jsonHelper.getBooleanValue("fields.boolean");
        assertTrue(bb);
        bb = this.jsonHelper.getBooleanValue("fields.boolean_str");
        assertTrue(bb);
    }

    @Test
    public void testClassSerialize() {
        TestModel model = new TestModel();
        System.out.println(model.toString());

        String json = JsonHelper.toJson(model);
        System.out.println(json);

        model = JsonHelper.toObject(json, TestModel.class);
        System.out.println(model.toString());
    }

    public static class TestModel {
        @JSONField(name="id", ordinal = 0)
        public Integer id = 1;

        @JSONField(name="name", ordinal = 1)
        public String name = "xiongsenlin";

        @JSONField(name="score", ordinal = 2)
        public Integer score = 100;

        @JSONField(name="date", ordinal = 3, format = "yyyy-MM-dd HH:mm:ss")
        public Date date = new Date();

        @JSONField(serialize = false, deserialize = false)
        public String others = "others";

        @Override
        public String toString() {
            return this.id + " " + this.name + " " + this.score + " " + this.date + " " + this.others;
        }
    }
}
