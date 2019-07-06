package hbase.util;

import org.apache.hadoop.hbase.TableName;

/**
 * 相关常量
 * @author lyd
 *
 */
public class HbaseConstant {
	/**
	 * hbase中的表名
	 */
	public static final TableName HBASE_TABLE_NAME_TN_USER = TableName.valueOf("userinfo");
	/**
	 * hbase中的表名   字符串类型
	 */

	public static final String HBASE_TABLE_NAME_STR_USER = "userinfo";


	/**
	 * hbasede的userinfos表的rowkey名
	 */
	public static final String HBASE_USER_ROWKEY = "id";

	/**
	 * hbase的userinfo表的列簇名
	 */
	public static final String HBASE_LOG_COLUMNFAMILY = "cfuser";

	/**
	 * hbase的userinfo表的列名
	 */
	public static final String HBASE_LOG_COLUMN_AGE = "age";

}
