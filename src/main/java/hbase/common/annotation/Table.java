package hbase.common.annotation;

import java.lang.annotation.*;

/**
 * @author jannal
 *表名注解
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Table {

	/**
	 * 表名
	 */
	String tableName();
	/**
	 * 列簇名
	 */
	String columnFamilyName();
	
}
