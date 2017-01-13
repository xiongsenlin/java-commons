package org.xsl.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by xiongsenlin on 17/1/11.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MysqlFieldAnnotation {
    /**
     * 字段在数据库中是否是主键
     * @return
     */
    boolean primaryKey() default false;

    /**
     * 字段对应的数据库中字段的名称
     * @return
     */
    String dbFieldName() default "";

    /**
     * 该字段是否允许为空
     * @return
     */
    boolean nullable() default true;

    /**
     * 仅对时间字段有效，表示时间的格式
     * @return
     */
    String format() default "yyyy-MM-dd HH:mm:ss";

    /**
     * 从数据库中获取数据的时候是否需要实例化该字段
     * @return
     */
    boolean fromDB() default true;

    /**
     * 是否需要将该字段实例化到数据库中
     * @return
     */
    boolean toDB() default true;
}
