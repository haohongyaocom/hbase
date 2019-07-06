package hbase.common;

import java.io.Serializable;

public class Car implements Serializable{
    
    private static final long serialVersionUID = 1L;

    private String carName;

    private Long carType;

    public String getCarName() {
        return carName;
    }

    public void setCarName(String carName) {
        this.carName = carName;
    }

    public Long getCarType() {
        return carType;
    }

    public void setCarType(Long carType) {
        this.carType = carType;
    }
    
    public static Car newDefaultCar(){
        Car car = new Car();
        car.setCarName("保时捷");
        car.setCarType(9L);
        return car;
    }

    @Override
    public String toString() {
        return "Car [carName=" + carName + ", carType=" + carType + "]";
    }
    
}
