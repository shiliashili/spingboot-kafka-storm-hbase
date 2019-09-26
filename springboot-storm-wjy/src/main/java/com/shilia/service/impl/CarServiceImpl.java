package com.shilia.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.shilia.dao.CarDao;
import com.shilia.entity.Car;
import com.shilia.service.CarService;

@Service
public class CarServiceImpl implements CarService{
	@Autowired
	private CarDao carDao;
	
	@Override
	public boolean insertCar(Car car) {
		// TODO 自动生成的方法存根
		return carDao.insertCar(car);
	}

	@Override
	public List<Car> findByCar() {
		// TODO 自动生成的方法存根
		return carDao.findByCar();
	}

	@Override
	public boolean delete(int id) {
		// TODO 自动生成的方法存根
		return carDao.delete(id);
	}

}
