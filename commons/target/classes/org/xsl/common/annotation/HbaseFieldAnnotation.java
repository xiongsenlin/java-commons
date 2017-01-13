package org.xsl.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by xiongsenlin on 15/7/8.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseFieldAnnotation {
    /**
     * 标记该字段是否是rowKey组成部分，在rowKey中的顺序与定义的顺宇一致
     * @return
     */
    boolean isRowKeyField() default false;

    /**
     * 表示rowKey的先后顺序，如果字段是rowKey，则必须设置该字段的值，从0开始
     * @return
     */
    int rowKeyIndex() default -1;

    /**
     * 表示该字段是否是hbase中列的组成部分
     * @return
     */
    boolean isDataField() default true;

    /**
     * 表示该字段对应hbase中字段的名称，两者可以不一致，如果不设置则表示名字与字段名字相同
     * @return
     */
    String mappingFiledName() default "";
}
