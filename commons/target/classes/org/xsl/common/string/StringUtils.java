package org.xsl.common.string;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xiongsenlin on 15/7/8.
 */
public class StringUtils {
    private static final String DEFAULT_CHARSET;

    static {
        DEFAULT_CHARSET = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "1234567890";
    }

    /**
     * 获取指定长度的随机字符串
     * @param length  返回字符串的长度
     * @param charset 字符集，默认字符集包含英文字符和数字
     * @return
     */
    public static String getRandomStr(int length, String charset) {
        String tmpCharset = charset;
        if(charset == null) {
            tmpCharset = DEFAULT_CHARSET;
        }

        Random random = new Random();
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < length; i++) {
            result.append(tmpCharset.charAt(random.nextInt(tmpCharset.length())));
        }

        return result.toString();
    }

    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /**
     * 判断给定的字符串和给定的模式是否完全匹配
     * @param pattern
     * @param str
     * @return
     */
    public static boolean isMatch(Pattern pattern, String str) {
        if (pattern == null || str == null) {
            return false;
        }

        Matcher matcher = pattern.matcher(str);
        return matcher.matches();
    }


    public static boolean isMatch(String patternStr, String str) {
        if (patternStr == null || str == null) {
            return false;
        }

        Pattern pattern = Pattern.compile(patternStr);
        return isMatch(pattern, str);
    }

    /**
     * 判断在给定的字符串中是否能找到符合模式串的子串
     * @param pattern
     * @param str
     * @return
     */
    public static boolean isFind(Pattern pattern, String str) {
        if (pattern == null || str == null) {
            return false;
        }

        Matcher matcher = pattern.matcher(str);
        return matcher.find();
    }


    public static boolean isFind(String patternStr, String str) {
        if (patternStr == null || str == null) {
            return false;
        }

        Pattern pattern = Pattern.compile(patternStr);
        return isFind(pattern, str);
    }

    /**
     * 返回给定字符串中匹配模式串的所有子串
     * @param pattern
     * @param str
     * @return
     */
    public static List<String> find(Pattern pattern, String str) {
        List<String> result = new ArrayList<String>();
        if (pattern == null || str == null) {
            return result;
        }

        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            result.add(matcher.group());
        }
        return result;
    }

    public static List<String> find(String patternStr, String str) {
        if (patternStr == null || str == null) {
            return new ArrayList<>();
        }

        Pattern pattern = Pattern.compile(patternStr);
        return find(pattern, str);
    }

    /**
     * 返回各分组的匹配结果
     * @param pattern
     * @param str
     * @return
     */
    public static List<String> groups(Pattern pattern, String str) {
        List<String> result = new ArrayList<>();
        if (pattern == null || str == null) {
            return result;
        }

        Matcher matcher = pattern.matcher(str);
        if(!matcher.matches()) {
            return result;
        }

        int groupCount = matcher.groupCount();
        for (int i = 1; i <= groupCount; i++) {
            result.add(matcher.group(i));
        }
        return result;
    }

    public static List<String> groups(String patternStr, String str) {
        if (patternStr == null || str == null) {
            return new ArrayList<>();
        }

        Pattern pattern = Pattern.compile(patternStr);
        return groups(pattern, str);
    }

    /**
     * 将按某种分隔符分割的字符串转换成字符数组，去掉空串
     * @param str
     * @param separator
     * @return
     */
    public static String [] stringToArray(String str, String separator) {
        if (isEmpty(str)) {
            return new String [0];
        }

        String [] tmpArray = str.split(separator);
        List<String> tmpList = new ArrayList<>();
        for (String item : tmpArray) {
            item = item.trim();
            if (!item.isEmpty()) {
                tmpList.add(item);
            }
        }

        String [] result = new String[tmpList.size()];
        for (int i = 0; i < tmpList.size(); i++) {
            result[i] = tmpList.get(i);
        }
        return result;
    }

    public static <E> String join(Collection<E> data, String separator) {
        StringBuilder sb = new StringBuilder();
        for (E item : data) {
            if (sb.length() > 0) {
                sb.append(separator);
            }

            if (item instanceof String) {
                sb.append((String) item);
            } else {
                sb.append(item.toString());
            }
        }

        return sb.toString();
    }

    public static <E> String join(E[] array, String separator) {
        StringBuilder sb = new StringBuilder();
        for (E item : array) {
            if (sb.length() > 0) {
                sb.append(separator);
            }

            if (item instanceof String) {
                sb.append((String) item);
            } else {
                sb.append(item.toString());
            }
        }

        return sb.toString();
    }
}
