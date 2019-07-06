package hbase.common;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.io.IOException;
import java.util.List;

public interface HbaseService {


    <T> List<T> findAll(final Class<T> clazz) ;

    <T> List<T> findAll(final Class<T> clazz,Filter... filter) ;


    <T> T findOneByRowKeyValue(final Class<T> clazz, String rowNameValue) ;


    <T> List<T> findFromStartToEndRowKey(final Class<T> clazz, String startRowKey, String endRowKey);

    <T> List<T> findFromStartToEndRowKey(final Class<T> clazz, String startRowKey, String endRowKey,Filter... filter) ;


    <T> List<T> findByPrefixRowKey(final Class<T> clazz,String prefixRowKey) throws Exception;

    <T> List<T> findByPrefixRowKey(final Class<T> clazz,String prefixRowKey,Filter... filter) ;

/*    <T> int getTotalRecord(final Class<T> clazz);

    <T> int getTotalRecord(final Class<T> clazz,String startRowKey, String endRowKey);*/

}
