package hbase.common;



import hbase.common.annotation.Column;
import hbase.common.annotation.Table;

import java.io.Serializable;

@Table(tableName="user",columnFamilyName="cfuser1")
public class User2 implements Serializable{
    private static final long serialVersionUID = -6279121344468801101L;
    @Column(columnName="userId",isRowName=true)
    private String userId;
    @Column(columnName="age")
    private int age;
    @Column(columnName="email")
    private String email;
    @Column(columnName="car")
    private Car car = Car.newDefaultCar();

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Car getCar() {
        return car;
    }

    public void setCar(Car car) {
        this.car = car;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "User [userId=" + userId + ", age=" + age + ", email=" + email + ", car=" + car + "]";
    }
    
    
}
