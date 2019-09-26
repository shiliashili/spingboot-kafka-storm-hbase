package com.shilia.service;

import java.util.List;

import com.shilia.entity.Car;

public interface CarService {
	/**
	 * 输入数据
	 */
	boolean insertCar(Car car);
	
	/**
	 * 查询数据
	 */
	List<Car> findByCar();
	
	boolean delete(int id);
}
