package org.xsl.common.test.string;

import org.junit.Test;
import org.xsl.common.json.JsonHelper;
import org.xsl.common.string.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiongsenlin on 17/1/9.
 */
public class TestStringUtils {
    @Test
    public void testGetRandomStr() {
        System.out.println(StringUtils.getRandomStr(5, null));
        System.out.println(StringUtils.getRandomStr(5, "1234"));
    }

    @Test
    public void testFind() {
        String str = "abc abc abc abc";
        List<String> occurs = StringUtils.find("abc", str);
        System.out.println(JsonHelper.toJson(occurs));
    }

    @Test
    public void testGroup() {
        String str = "my test string.";
        List<String> groups = StringUtils.groups(".*(t).*(t).*", str);
        System.out.println(JsonHelper.toJson(groups));
    }

    @Test
    public void testStringToArray() {
        String str = "my test string";
        String [] array = StringUtils.stringToArray(str, " ");
        System.out.println(JsonHelper.toJson(array));
    }

    @Test
    public void testJoin() {
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");
        System.out.println(StringUtils.join(list, ", "));

        String [] array = {"a", "b", "c"};
        System.out.println(StringUtils.join(array, ", "));
    }
}
