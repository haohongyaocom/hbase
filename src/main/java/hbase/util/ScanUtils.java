package hbase.util;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanUtils {


    /**
     * @param filters   单列值过滤器
     * @return  扫描器
     */
    public static Scan getRowsScan( Filter... filters) {
        Scan scan = new Scan();
        FilterList fl = new FilterList();
        for (int i =0;i<filters.length;i++){
            fl.addFilter(filters[i]);
        }
        scan.setFilter(fl);
        return scan;
    }

    /**
     * @param filters   单列值过滤器
     *  @param   fields         要获取的字段名
     * @return  扫描器
     */
    public static Scan getRowsScan(String[] fields,Filter... filters) {
        Scan scan = new Scan();
        FilterList fl = new FilterList();
        for (int i =0;i<filters.length;i++){
            fl.addFilter(filters[i]);
        }
        fl.addFilter(getFilters(fields));
        scan.setFilter(fl);
        return scan;
    }


    /**
     * @param startRow  起始rowKey
     * @param endRow     结束rowKey
     * @return  扫描器
     */
    public static Scan getRowsScan(String startRow, String endRow) {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
       // scan.setMaxResultSize(2);
        scan.setStopRow(Bytes.toBytes(endRow));
        return scan;
    }


    /**
     *
     * @param startRow  起始rowKey
     * @param endRow     结束rowKey
     * @param filters   单列值过滤器
     * @return  扫描器
     */
    public static Scan getRowsScan(String startRow, String endRow, Filter... filters) {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        FilterList fl = new FilterList();
        for (int i =0;i<filters.length;i++){
            fl.addFilter(filters[i]);
        }
        scan.setFilter(fl);
        return scan;
    }


    /**
     * @param startRow   起始rowkey
     * @param endRow   结束rowkey
     * @param fields   要获取的字段
     * @return   扫描器
     * @throws Exception
     */
    public static Scan getRowsScan(String startRow, String endRow, String[] fields) {
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));

        FilterList fl = new FilterList();
        fl.addFilter(getFilters(fields));
        scan.setFilter(fl);
        return scan;
    }
    /**
     *
     * @param startRow  起始rowKey
     * @param endRow     结束rowKey
     * @param filters   单列值过滤器
     *  @param   fields         要获取的字段名
     * @return  扫描器
     */
    public static Scan getRowsScan(String startRow, String endRow, String[] fields,Filter... filters) {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        FilterList fl = new FilterList();
        for (int i =0;i<filters.length;i++){
            fl.addFilter(filters[i]);
        }
        fl.addFilter(getFilters(fields));
        scan.setFilter(fl);
        return scan;
    }

    /**
     * @param prefixRow   rowKey的前缀
     * @return     以rowKey前缀的数据的扫描器
     * @throws Exception
     */
    public static Scan getRowsScan(String prefixRow) {
        Scan scan = new Scan();
        Filter pf = new PrefixFilter(Bytes.toBytes(prefixRow));
        scan.setFilter(pf);
        return scan;
    }

    /**
     * 通过rowkwy的前缀查询出满足单列过滤器的数据
     * @param prefixRow   rowKey的前缀
     * @param filters  单列过滤器数组
     * @return
     * @throws Exception
     */
    public static Scan getPrefixRowsScan(String prefixRow,Filter... filters) {
        Scan scan = new Scan();
        Filter pf = new PrefixFilter(Bytes.toBytes(prefixRow));
        FilterList fl = new FilterList();
        fl.addFilter(pf);
        for (int i =0;i<filters.length;i++){
            fl.addFilter(filters[i]);
        }
        scan.setFilter(fl);
        return scan;
    }


    /**
     * 通过startRow和endRow查找一些指定字段的扫描器
     * @param prefixRow  rowKey的前缀
     * @param fields     要获取的字段
     * @return
     * @throws Exception
     */
    public static Scan getPrefixRowsScan(String prefixRow, String[] fields) {
        Scan scan = new Scan();
        FilterList fl = new FilterList();
        Filter pf = new PrefixFilter(Bytes.toBytes(prefixRow));
        fl.addFilter(pf);
        fl.addFilter(getFilters(fields));
        scan.setFilter(fl);
        return scan;
    }

    /**
     * 通过rowkwy的前缀查询，满足一个单列过滤器的一些指定字段
     * @param prefixRow   rowKey的前缀
     * @param filters  单列过滤器
     * @param fields  要获取的字段
     * @return
     * @throws Exception
     */
    public static Scan getPrefixRowsScan(String prefixRow, String[] fields,Filter... filters) {
        Scan scan = new Scan();
        FilterList fl = new FilterList();
        Filter pf = new PrefixFilter(Bytes.toBytes(prefixRow));
        fl.addFilter(pf);
        for (int i =0;i<filters.length;i++){
            fl.addFilter(filters[i]);
        }
        fl.addFilter(getFilters(fields));
        scan.setFilter(fl);
        return scan;
    }

    /**
     * 随机抽取一部分数据的扫描器
     * @param f
     * @return
     * @throws Exception
     */
    public static Scan getRowsScan(float f)
            throws Exception {
        Scan scan = new Scan();
        Filter rrf = new RandomRowFilter(f);
        scan.setFilter(rrf);
        return scan;
    }

    /**
     * 设置扫描的列
     * @param fields  要扫描的列名
     * @return   一个过滤器
     */
    private static Filter getFilters(String[] fields) {
        int length = fields.length;
        byte[][] filters = new byte[length][];
        for (int i=0;i<length;i++){
            filters[i] = Bytes.toBytes(fields[i]);
        }
        return new MultipleColumnPrefixFilter(filters);
    }



}
