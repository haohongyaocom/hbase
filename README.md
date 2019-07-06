问题:
表中有非string类型  比如int    需要先判断类型再去转化为相应的类型

关于   GREATER_OR_EQUAL  查询与理想结果不同的问题


rowkey正则

分页



# 基于注解的Hbase API
## 配置
### config.properties


```java
```

### applicationContext-hbase.xml

### applicationContext-all.xml


### 注解详解
`@Table`

|属性 | 说明 | 默认值|
|----|------|----|
|tableName| 表名  | |
|columnFamilyName |  列簇名  | |


@Column

|属性 | 说明 | 默认值|
|----|------|----|
|columnName| 列名| |
|isRowName |  列簇名  | false|



