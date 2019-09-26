package com.shilia.dao;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import com.shilia.entity.Car;

@Mapper
public interface CarDao {
	/**
	 * 输入数据
	 */
	@Insert("INSERT INTO car_table(name,usr) VALUES(#{name},#{usr})")
	boolean insertCar(Car car);
	
	/**
	 * 查询数据
	 */
	@Select("SELECT *from car_table")
	List<Car> findByCar();
	
	@Delete("DELETE from car_table WHERE id = #{id}")
	boolean delete(int id);
}
