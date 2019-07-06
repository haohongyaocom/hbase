package hbase.common;



import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spring.JUnitActionBase;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase api单元测试用例
 * @author jannal
 */
public class HbaseServiceImplTest  extends JUnitActionBase {

    private static final Logger logger = LoggerFactory.getLogger(HbaseServiceImplTest.class);

    
    @Resource
    private HbaseService hbaseService;
    /**
     * 单一对象测试存储
     * hbase shell中通过scan "person" 查询是否正确
     */

    

    

    /**
     * 插入多条数据并查询
     */
    @Test
    public void testFindAll2(){

        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("cfuser"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                new SubstringComparator("14"));
        //这个大于的比较器为什么不能比较
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                Bytes.toBytes("cfuser"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new SubstringComparator("11"));
        List<User> userList = hbaseService.findAll(User.class);
        logger.info("user表的信息如下:{}",userList);
        for (User user:userList) {
            System.out.println(user);
        }
    }



    /**
     * rowkey查询
     */
    @Test
    public void testFindOneByRowKeyValue(){
        long startTime = System.nanoTime();
        User user = hbaseService.findOneByRowKeyValue(User.class, "001");
        System.out.println(user);
        long endTime = System.nanoTime();
        logger.info("user:{},查询一条数据花费时间",user,(endTime-startTime)/(1000));
    }


    /**
     * rowkey范围查询
     */
    @Test
    public void testFindFromStartToEndRowKey() throws Exception {
        // List<User> list = hbaseService.findFromStartToEndRowKey(User.class,"001","004");
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("cfuser"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                new SubstringComparator("14"));
        //这个大于的比较器为什么不能比较
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                Bytes.toBytes("cfuser"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new SubstringComparator("12"));
        //List<User> list = hbaseService.findFromStartToEndRowKey(User.class,"001","004");
        List<User> list = hbaseService.findByPrefixRowKey(User.class,"00");
        for (User user:list) {
            System.out.println(user);
        }
        logger.info("数据大小:{}",list.size());
        logger.info("数据{}",list);
    }



}