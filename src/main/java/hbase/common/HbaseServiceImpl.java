package hbase.common;

         import hbase.util.ScanUtils;
        import hbase.common.annotation.Column;
        import hbase.common.annotation.Table;
        import hbase.serialization.StringHbaseSerializer;
         import org.apache.hadoop.hbase.TableName;
         import org.apache.hadoop.hbase.client.*;
         import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
         import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
         import org.apache.hadoop.hbase.filter.Filter;
        import org.apache.hadoop.hbase.util.Bytes;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;
        import org.springframework.beans.factory.annotation.Autowired;
        import org.springframework.data.hadoop.hbase.HbaseTemplate;
        import org.springframework.data.hadoop.hbase.ResultsExtractor;
        import org.springframework.data.hadoop.hbase.RowMapper;
        import org.springframework.stereotype.Service;
        import org.springframework.util.ReflectionUtils;

        import java.io.IOException;
        import java.io.Serializable;
        import java.lang.reflect.Field;
        import java.lang.reflect.Modifier;
        import java.util.*;
        import java.util.Map.Entry;

/**
 * hbase 通c 用处理方法
 *
 * @author jannal
 */
@Service
@SuppressWarnings("all")
public class HbaseServiceImpl implements HbaseService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String ROW_NAME = "rowName";
    @Autowired
    private HbaseTemplate hbaseTemplate;




    private <T extends Serializable> void checkTableAnnotation(final T t, final Table table) {
        if (table == null) {
            throw new IllegalArgumentException("请检查" + t.getClass().getName() + "注解@Table是否添加");
        }
    }



    public void checkTableAndEnable(final String tableName, HBaseAdmin admin) throws IOException {
        boolean tableAvailable = admin.isTableAvailable(tableName);
        boolean tableEnabled = admin.isTableEnabled(tableName);
        if (tableAvailable && tableEnabled) {
            // 如果表不可用，先置为可用
            admin.enableTable(tableName);
        }
    }

    /**
     * 查找最近一个版本的一条数据 通过rowName的值查找 rowkey必须是字符串，并且是字符串序列化方式才可以使用此方法获取
     */
    public <T> T findOneByRowKeyValue(final Class<T> clazz, String rowNameValue) {
        final Table table = clazz.getAnnotation(Table.class);
        if (table != null) {
            try {
                final String columnFamilyName = table.columnFamilyName();
                final String tableName = table.tableName();
                final T newInstance = clazz.newInstance();
                System.out.println(newInstance.toString());
                final Map<Field/* field */, byte[]/* columnName值 */> map = new HashMap<Field, byte[]>();
                // 遍历field
                ReflectionUtils.doWithLocalFields(clazz, new ReflectionUtils.FieldCallback() {
                    @Override
                    public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {

                        if (field.isAnnotationPresent(Column.class)) {
                            int modifiers = field.getModifiers();
                            if (Modifier.isStatic(modifiers)) {
                                return;
                            }
                            field.setAccessible(true);
                            Column column = field.getAnnotation(Column.class);
                            // map.put(field, Bytes.toBytes(column.columnName()));
                            map.put(field,new StringHbaseSerializer().serialize(column.columnName()+""));
                        }
                    }
                });
                System.out.println(map);

                T t = hbaseTemplate.get(tableName, rowNameValue, columnFamilyName, new RowMapper<T>() {
                    @Override
                    public T mapRow(Result result, int rowNum) throws Exception {
                        byte[] row = result.getRow();
                        System.out.println(new String(row));
                        //防止当T中的属性有初始化值时，是可以获取到的对象数据的，但是在hbase中是没有数据的
                        if(row==null&&result.isEmpty()){
                            return null;
                        }

                        Set<Entry<Field, byte[]>> entrySet = map.entrySet();
                        Iterator<Entry<Field, byte[]>> iterator = entrySet.iterator();
                        while (iterator.hasNext()) {
                            Entry<Field, byte[]> entry = iterator.next();
                            Field field = entry.getKey();
                            byte[] columnName = entry.getValue();
                            byte[] fieldValue = result.getValue(Bytes.toBytes(columnFamilyName), columnName);
                            // field.set(newInstance, new StringHbaseSerializer().deserialize(row));
                            if (fieldValue != null) {
                                // field.set(newInstance, SerializationUtils.deserialize(fieldValue));
                                field.set(newInstance, new StringHbaseSerializer().deserialize(fieldValue));
                            }
                        }
                        return newInstance;
                    }
                });
                return t;
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

        } else {
            logger.warn("{}没有指定@Table注解", clazz.getName());
        }
        return null;
    }



    /**
     * 目前不支持静态属性
     */
    public <T> List<T> findAll(final Class<T> clazz) {
        final Table table = clazz.getAnnotation(Table.class);
        if (table != null) {
            final String columnFamilyName = table.columnFamilyName();
            final String tableName = table.tableName();

            final Map<String /* field 名称 */, byte[]/* field 值 */> map = new HashMap<String, byte[]>();
            List<T> list = hbaseTemplate.find(tableName, columnFamilyName, new RowMapper<T>() {
                @Override
                public T mapRow(final Result result, int rowNum) throws Exception {
                    final T newInstance = clazz.newInstance();
                    ReflectionUtils.doWithLocalFields(clazz, new ReflectionUtils.FieldCallback() {
                        @Override
                        public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                            int modifiers = field.getModifiers();
                            if (Modifier.isStatic(modifiers)) {
                                return;// 忽略静态属性
                            }
                            if (Modifier.isPrivate(modifiers)) {
                                field.setAccessible(true);
                            }

                            Column column = field.getAnnotation(Column.class);
                            map.put(field.getName(), result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(column.columnName())));
                            // 属性赋值
                            byte[] fieldValue = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(field.getName()));
                            if (fieldValue != null) {
                                field.set(newInstance, new StringHbaseSerializer().deserialize(fieldValue));
                            }
                        }

                    });
                    return newInstance;
                }
            });

            return list;
        } else {
            logger.warn("{}没有指定@Table注解", clazz.getName());
        }
        return Collections.EMPTY_LIST;

    }

    @Override
    public <T> List<T> findAll(final Class<T> clazz, Filter... filter) {
        Scan scan = ScanUtils.getRowsScan(filter);

        if (clazz == null) {
            throw new IllegalArgumentException(clazz + "对象不能为空");
        }
        final Table table = clazz.getAnnotation(Table.class);
        if (table == null) {
            throw new IllegalArgumentException("请检查" + clazz.getName() + "注解@Table是否添加");
        }

        final List<T> list = new ArrayList<T>();
        final String columnFamilyName = table.columnFamilyName();
        final String tableName = table.tableName();
        final Map<String /* field 名称 */, byte[]/* field 值 */> map = new HashMap<String, byte[]>();

        hbaseTemplate.find(tableName, scan, new ResultsExtractor<T>() {
            @Override
            public T extractData(ResultScanner results) throws Exception {

                for (final Result result : results) {
                    final T newInstance = clazz.newInstance();
                    ReflectionUtils.doWithLocalFields(clazz, new ReflectionUtils.FieldCallback() {
                        @Override
                        public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                            int modifiers = field.getModifiers();
                            if (Modifier.isStatic(modifiers)) {
                                return;// 忽略静态属性
                            }
                            if (Modifier.isPrivate(modifiers)) {
                                field.setAccessible(true);
                            }

                            Column column = field.getAnnotation(Column.class);
                            map.put(field.getName(), result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(column.columnName())));
                            // 属性赋值
                            byte[] fieldValue = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(field.getName()));
                            if (fieldValue != null) {
                                field.set(newInstance, new StringHbaseSerializer().deserialize(fieldValue));
                            }
                        }

                    });
                    list.add(newInstance);
                }
                return null;
            }

        });
        return list;
    }

    public <T> List<T> findFromStartToEndRowKey(final Class<T> clazz, String startRowKey, String endRowKey) {
        Scan scan = ScanUtils.getRowsScan(startRowKey,endRowKey);

        if (clazz == null) {
            throw new IllegalArgumentException(clazz + "对象不能为空");
        }
        final Table table = clazz.getAnnotation(Table.class);
        if (table == null) {
            throw new IllegalArgumentException("请检查" + clazz.getName() + "注解@Table是否添加");
        }

        final List<T> list = new ArrayList<T>();
        final String columnFamilyName = table.columnFamilyName();
        final String tableName = table.tableName();
        final Map<String /* field 名称 */, byte[]/* field 值 */> map = new HashMap<String, byte[]>();

        hbaseTemplate.find(tableName, scan, new ResultsExtractor<T>() {
            @Override
            public T extractData(ResultScanner results) throws Exception {

                for (final Result result : results) {
                    final T newInstance = clazz.newInstance();
                    ReflectionUtils.doWithLocalFields(clazz, new ReflectionUtils.FieldCallback() {
                        @Override
                        public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                            int modifiers = field.getModifiers();
                            if (Modifier.isStatic(modifiers)) {
                                return;// 忽略静态属性
                            }
                            if (Modifier.isPrivate(modifiers)) {
                                field.setAccessible(true);
                            }

                            Column column = field.getAnnotation(Column.class);
                            map.put(field.getName(), result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(column.columnName())));
                            // 属性赋值
                            byte[] fieldValue = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(field.getName()));
                            if (fieldValue != null) {
                                field.set(newInstance, new StringHbaseSerializer().deserialize(fieldValue));
                            }
                        }

                    });
                    list.add(newInstance);
                }
                return null;
            }

        });
        return list;
    }

    public <T> List<T> findFromStartToEndRowKey(final Class<T> clazz, String startRowKey, String endRowKey,Filter... filter) {

        Scan scan = ScanUtils.getRowsScan(startRowKey,endRowKey,filter);

        if (clazz == null) {
            throw new IllegalArgumentException(clazz + "对象不能为空");
        }
        final Table table = clazz.getAnnotation(Table.class);
        if (table == null) {
            throw new IllegalArgumentException("请检查" + clazz.getName() + "注解@Table是否添加");
        }

        final List<T> list = new ArrayList<T>();
        final String columnFamilyName = table.columnFamilyName();
        final String tableName = table.tableName();
        final Map<String /* field 名称 */, byte[]/* field 值 */> map = new HashMap<String, byte[]>();

        hbaseTemplate.find(tableName, scan, new ResultsExtractor<T>() {
            @Override
            public T extractData(ResultScanner results) throws Exception {

                for (final Result result : results) {
                    final T newInstance = clazz.newInstance();
                    ReflectionUtils.doWithLocalFields(clazz, new ReflectionUtils.FieldCallback() {
                        @Override
                        public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                            int modifiers = field.getModifiers();
                            if (Modifier.isStatic(modifiers)) {
                                return;// 忽略静态属性
                            }
                            if (Modifier.isPrivate(modifiers)) {
                                field.setAccessible(true);
                            }

                            Column column = field.getAnnotation(Column.class);
                            map.put(field.getName(), result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(column.columnName())));
                            // 属性赋值
                            byte[] fieldValue = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(field.getName()));
                            if (fieldValue != null) {
                                field.set(newInstance, new StringHbaseSerializer().deserialize(fieldValue));
                            }
                        }

                    });
                    list.add(newInstance);
                }
                return null;
            }

        });
        return list;
    }

    @Override
    public <T> List<T> findByPrefixRowKey(final Class<T> clazz, String prefixRowKey) {
        Scan scan = ScanUtils.getPrefixRowsScan(prefixRowKey);

        if (clazz == null) {
            throw new IllegalArgumentException(clazz + "对象不能为空");
        }
        final Table table = clazz.getAnnotation(Table.class);
        if (table == null) {
            throw new IllegalArgumentException("请检查" + clazz.getName() + "注解@Table是否添加");
        }

        final List<T> list = new ArrayList<T>();
        final String columnFamilyName = table.columnFamilyName();
        final String tableName = table.tableName();
        final Map<String /* field 名称 */, byte[]/* field 值 */> map = new HashMap<String, byte[]>();

        hbaseTemplate.find(tableName, scan, new ResultsExtractor<T>() {
            @Override
            public T extractData(ResultScanner results) throws Exception {

                for (final Result result : results) {
                    final T newInstance = clazz.newInstance();
                    ReflectionUtils.doWithLocalFields(clazz, new ReflectionUtils.FieldCallback() {
                        @Override
                        public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                            int modifiers = field.getModifiers();
                            if (Modifier.isStatic(modifiers)) {
                                return;// 忽略静态属性
                            }
                            if (Modifier.isPrivate(modifiers)) {
                                field.setAccessible(true);
                            }

                            Column column = field.getAnnotation(Column.class);
                            map.put(field.getName(), result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(column.columnName())));
                            // 属性赋值
                            byte[] fieldValue = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(field.getName()));
                            if (fieldValue != null) {
                                field.set(newInstance, new StringHbaseSerializer().deserialize(fieldValue));
                            }
                        }

                    });
                    list.add(newInstance);
                }
                return null;
            }

        });
        return list;
    }

    @Override
    public <T> List<T> findByPrefixRowKey(final Class<T> clazz, String prefixRowKey, Filter... filter) {
        Scan scan = ScanUtils.getPrefixRowsScan(prefixRowKey,filter);

        if (clazz == null) {
            throw new IllegalArgumentException(clazz + "对象不能为空");
        }
        final Table table = clazz.getAnnotation(Table.class);
        if (table == null) {
            throw new IllegalArgumentException("请检查" + clazz.getName() + "注解@Table是否添加");
        }

        final List<T> list = new ArrayList<T>();
        final String columnFamilyName = table.columnFamilyName();
        final String tableName = table.tableName();
        final Map<String /* field 名称 */, byte[]/* field 值 */> map = new HashMap<String, byte[]>();

        hbaseTemplate.find(tableName, scan, new ResultsExtractor<T>() {
            @Override
            public T extractData(ResultScanner results) throws Exception {

                for (final Result result : results) {
                    final T newInstance = clazz.newInstance();
                    ReflectionUtils.doWithLocalFields(clazz, new ReflectionUtils.FieldCallback() {
                        @Override
                        public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                            int modifiers = field.getModifiers();
                            if (Modifier.isStatic(modifiers)) {
                                return;// 忽略静态属性
                            }
                            if (Modifier.isPrivate(modifiers)) {
                                field.setAccessible(true);
                            }

                            Column column = field.getAnnotation(Column.class);
                            map.put(field.getName(), result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(column.columnName())));
                            // 属性赋值
                            byte[] fieldValue = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(field.getName()));
                            if (fieldValue != null) {
                                field.set(newInstance, new StringHbaseSerializer().deserialize(fieldValue));
                            }
                        }

                    });
                    list.add(newInstance);
                }
                return null;
            }

        });
        return list;
    }


    private void fieldPreHandle(Field field) {
        int modifiers = field.getModifiers();
        if (Modifier.isStatic(modifiers)) {
            throw new IllegalStateException("@Column注解不被支持在static属性上，因为static无法序列化");
        }
        if (Modifier.isPrivate(modifiers)) {
            field.setAccessible(true);
        }
    }

}
