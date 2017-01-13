package org.xsl.common.util;

import org.xsl.common.json.JsonHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 对map数据进行过滤，只支持数值类型和字符串类型数据的过滤功能，支持的比较符号有：
 * >, <, =, !=, >=, <=, 使用说明如下：首先声明一个{@link Criteria}对象，用来
 * 描述过滤条件，具体的使用方法参考该类的说明，然后用该对应初始化一个{@link MapDataFilter}
 * 对象，然后调用该对象的{@link MapDataFilter#filter(Map)}方法进行数据的过滤功能：
 *
 * -------------------------------------------------------------------------------------
 *     MapDataFilter.Criteria criteria = new MapDataFilter.Criteria();
 *     criteria.or().and(new MapDataFilter.CriteriaItem("f1", "int", ">", "10"))
 *                  .and(new MapDataFilter.CriteriaItem("f2", "float", "<=", "23"))
 *             .or().and(new MapDataFilter.CriteriaItem("f3", "long", ">", "20"))
 *                  .and(new MapDataFilter.CriteriaItem("f3", "long", "<", "30"));
 *     MapDataFilter mapDataFilter = new MapDataFilter(criteria);
 *     mapDataFilter.filter(map);
 * --------------------------------------------------------------------------------------
 *
 * Created by xiongsenlin on 15/8/11.
 */
public class MapDataFilter {
    private List<List<Comparable>> comparators;

    public MapDataFilter(Criteria criteria) {
        this.compile(criteria);
    }

    /**
     * 通过json格式的配置字符串完成初始化
     * @param config  config格式为双层数组结构，外层数组中的元素之间的逻辑关系为OR，内层数据元素之间的关系为AND
     *                例如：[ [r1, r2, r3], [r4, r5], [r6] ]表示的关系为: (r1 && r2 && r3) || (r4 && r5)
     *                || (r6)。其中r1...r6表示一个比较关系，为一个map结构，例如：{"field": "abc", "type": "int",
     *                "comparator": ">", "threshold": "6"}表示的条件为：abc > 6，其中abc表示字段的名称，类型为int。
     */
    public MapDataFilter(String config) {
        Criteria criteria = new Criteria();

        List orList = JsonHelper.toObject(config, List.class);
        for (Object orItem : orList) {
            criteria.or();

            if (orItem instanceof List) {
                List andList = (List) orItem;

                for (Object andItem : andList) {
                    if (andItem instanceof Map) {
                        Map fieldFilter = (Map) andItem;
                        String field = (String) fieldFilter.get("field");
                        String type = (String) fieldFilter.get("type");
                        String operator = (String) fieldFilter.get("comparator");
                        String threshold = (String) fieldFilter.get("threshold");
                        MapDataFilter.CriteriaItem criteriaItem = new MapDataFilter.CriteriaItem(field, type, operator, threshold);
                        criteria.and(criteriaItem);
                    }
                    else {
                        throw new RuntimeException("Data filter config is invalid");
                    }
                }
            }
            else {
                throw new RuntimeException("Data filter config is invalid");
            }
        }

        this.compile(criteria);
    }

    /**
     * 对数据进行过滤，符合过滤条件的数据返回true
     * @param data
     * @return
     */
    public boolean filter(Map data) {
        if (this.comparators == null || this.comparators.size() == 0) {
            return true;
        }
        else {
            for (List<Comparable> list : this.comparators) {
                boolean flag = true;
                for (Comparable comparable : list) {
                    if (comparable.compareTo(data) != 1) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    return true;
                }
            }
        }
        return false;
    }

    private void compile(Criteria criteria) {
        if (criteria == null) {
            return;
        }

        this.comparators = new ArrayList<List<Comparable>>();
        List<List<CriteriaItem>> tmpList = criteria.getCriteria();

        for (List<CriteriaItem> list : tmpList) {
            List<Comparable> comparableList = new ArrayList<Comparable>();
            for (CriteriaItem item : list) {
                String type = item.fieldType.toLowerCase();

                Comparable comparable;
                if ("double".equals(type) || "float".equals(type)) {
                    comparable = new DoubleComparator(item.threshold, item.comparator, item.fieldName);
                }
                else if ("int".equals(type) || "long".equals(type)) {
                    comparable = new LongComparator(item.threshold, item.comparator, item.fieldName);
                }
                else {
                    comparable = new StringComparator(item.threshold, item.comparator, item.fieldName);
                }
                comparableList.add(comparable);
            }
            this.comparators.add(comparableList);
        }
    }

    /**
     * 数据过滤条件，{@link #criteria}是双层list，外层list中的元素在语义上是 OR 的关系内层
     * list在语义上是 AND 的关系，假设要实现: (c1 && c2 && c3) || (c4 && c5) || c6 的
     * 过滤条件，只需要执行以下操作：
     * -------------------------------------------------------------------------
     *     Criteria criteria = new Criteria().or().and(c1).and(c2).and(c3)
     *                                       .or().and(c4).and(c5)
     *                                       .or().and(c6)
     * -------------------------------------------------------------------------
     */
    public static class Criteria {
        private List<List<CriteriaItem>> criteria;

        public Criteria() {
            this.criteria = new ArrayList<>();
        }

        public synchronized Criteria or() {
            List<CriteriaItem> criteriaItems = new ArrayList<CriteriaItem>();
            this.criteria.add(criteriaItems);
            return this;
        }

        public synchronized Criteria and(CriteriaItem item) {
            if (this.criteria.size() == 0) {
                throw new RuntimeException("Before call this method, you should call or() first");
            }
            else {
                this.criteria.get(this.criteria.size() - 1).add(item);
            }
            return this;
        }

        public List<List<CriteriaItem>> getCriteria() {
            return criteria;
        }

        public void setCriteria(List<List<CriteriaItem>> criteria) {
            this.criteria = criteria;
        }
    }

    /**
     * 最细力度的过滤条件，设计具体某个字段的过滤条件
     */
    public static class CriteriaItem {
        private static List<String> supportedType = Arrays.asList("double", "float", "int", "long", "string");
        private static List<String> getSupportedComparator = Arrays.asList(">", "<", ">=", "<=", "=", "!=");

        /**
         * 字段名称
         */
        private String fieldName;

        /**
         * 字段类型，只支持：double, float, int, long, string
         */
        private String fieldType;

        /**
         * 比较运算符，只支持：>, <, >=, <=, =, !=
         */
        private String comparator;

        /**
         * 被比较的阈值
         */
        private String threshold;

        public CriteriaItem(String field, String type, String comparator, String threshold) {
            if (!supportedType.contains(type)) {
                throw new RuntimeException("Data type not supported yet");
            }
            if (!getSupportedComparator.contains(comparator)) {
                throw new RuntimeException("Operator not supported yet");
            }

            this.fieldName = field;
            this.fieldType = type;
            this.comparator = comparator;
            this.threshold = threshold;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldType() {
            return fieldType;
        }

        public void setFieldType(String fieldType) {
            this.fieldType = fieldType;
        }

        public String getComparator() {
            return comparator;
        }

        public void setComparator(String comparator) {
            this.comparator = comparator;
        }

        public String getThreshold() {
            return threshold;
        }

        public void setThreshold(String threshold) {
            this.threshold = threshold;
        }
    }

    private class DoubleComparator implements Comparable<Map<String, Object>> {
        private double value;
        private String field;
        private String comparator;

        public DoubleComparator(String threshold, String comparator, String field) {
            this.comparator = comparator;
            this.field = field;
            this.value = Double.parseDouble(threshold);
        }

        @Override
        public int compareTo(Map<String, Object> map) {
            Object o = map.get(this.field);
            if (o == null) {
                return 0;
            }

            double tValue;
            if (o instanceof Double || o instanceof Float) {
                if (o instanceof Float) {
                    tValue = ((Float) o).doubleValue();
                } else {
                    tValue = ((Double) o).doubleValue();
                }
            } else {
                tValue = Double.parseDouble(o.toString());
            }

            if ("=".equals(this.comparator)) {
                return this.value == tValue ? 1 : 0;
            } else if ("!=".equals(this.comparator)) {
                return this.value != tValue ? 1 : 0;
            } else if ("<".equals(this.comparator)) {
                return tValue < this.value ? 1 : 0;
            } else if ("<=".equals(this.comparator)) {
                return tValue <= this.value ? 1 : 0;
            } else if (">".equals(this.comparator)) {
                return tValue > this.value ? 1 : 0;
            } else if (">=".equals(this.comparator)) {
                return tValue >= this.value ? 1 : 0;
            } else {
                return 0;
            }
        }
    }

    private class LongComparator implements Comparable<Map<String, Object>> {
        private long value;
        private String field;
        private String comparator;

        public LongComparator(String threshold, String comparator, String field) {
            this.comparator = comparator;
            this.field = field;
            this.value = Long.parseLong(threshold);
        }

        @Override
        public int compareTo(Map<String, Object> map) {
            Object o = map.get(this.field);
            if (o == null) {
                return 0;
            }

            long tValue;
            if (o instanceof Long || o instanceof Integer) {
                if (o instanceof Integer) {
                    tValue = ((Integer) o).longValue();
                } else {
                    tValue = ((Long) o).longValue();
                }
            } else {
                tValue = Long.parseLong(o.toString());
            }

            if ("=".equals(this.comparator)) {
                return this.value == tValue ? 1 : 0;
            } else if ("!=".equals(this.comparator)) {
                return this.value != tValue ? 1 : 0;
            } else if ("<".equals(this.comparator)) {
                return tValue < this.value ? 1 : 0;
            } else if ("<=".equals(this.comparator)) {
                return tValue <= this.value ? 1 : 0;
            } else if (">".equals(this.comparator)) {
                return tValue > this.value ? 1 : 0;
            } else if (">=".equals(this.comparator)) {
                return tValue >= this.value ? 1 : 0;
            } else {
                return 0;
            }
        }
    }

    private class StringComparator implements Comparable<Map<String, Object>> {
        private String value;
        private String field;
        private String comparator;

        public StringComparator(String threshold, String comparator, String field) {
            this.comparator = comparator;
            this.field = field;
            this.value = threshold;
        }

        @Override
        public int compareTo(Map<String, Object> map) {
            Object o = map.get(this.field);
            if (o == null) {
                return 0;
            }

            String tValue = o.toString();
            if ("=".equals(this.comparator)) {
                return this.value.equals(tValue) ? 1 : 0;
            } else if ("!=".equals(this.comparator)) {
                return !this.value.equals(tValue) ? 1 : 0;
            } else if ("<".equals(this.comparator)) {
                return tValue.compareTo(this.value) < 0 ? 1 : 0;
            } else if ("<=".equals(this.comparator)) {
                return tValue.compareTo(this.value) <= 0 ? 1 : 0;
            } else if (">".equals(this.comparator)) {
                return tValue.compareTo(this.value) > 0 ? 1 : 0;
            } else if (">=".equals(this.comparator)) {
                return tValue.compareTo(this.value) >= 0 ? 1 : 0;
            } else {
                return 0;
            }
        }
    }
}
