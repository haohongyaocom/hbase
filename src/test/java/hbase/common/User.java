package hbase.common;



import hbase.util.HbaseConstant;
import hbase.common.annotation.Column;
import hbase.common.annotation.Table;

import java.io.Serializable;

@Table(tableName=HbaseConstant.HBASE_TABLE_NAME_STR_USER,columnFamilyName= HbaseConstant.HBASE_LOG_COLUMNFAMILY)
public class User implements Serializable{
    private static final long serialVersionUID = -6279121344468801101L;
    @Column(columnName=HbaseConstant.HBASE_USER_ROWKEY,isRowName=true)
    private String userId;
    @Column(columnName=HbaseConstant.HBASE_LOG_COLUMN_AGE)
    private String age;


    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "User [userId=" + userId + ", age=" + age  + "]";
    }
    
    public String getType(String columnName){
        return  null;
    }
}
