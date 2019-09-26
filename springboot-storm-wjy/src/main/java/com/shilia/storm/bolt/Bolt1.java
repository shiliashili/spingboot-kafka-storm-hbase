package com.shilia.storm.bolt;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.shilia.entity.Car;
import com.shilia.service.CarService;
import com.shilia.util.GetBean;

public class Bolt1 extends BaseRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5189432094311645675L;
	
	private static final Logger logger = LoggerFactory.getLogger(Bolt1.class);
	
	private CarService carService;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO 自动生成的方法存根
		carService = GetBean.getBean(CarService.class);
	}

	@Override
	public void execute(Tuple input) {
		// TODO 自动生成的方法存根
		String msg = input.getStringByField("bolt1");
		try {
			List<Car> listcar = JSON.parseArray(msg,Car.class);
			if (listcar != null && listcar.size()>0) {
				Iterator<Car> iterator = listcar.iterator();
				while(iterator.hasNext()) {
					Car car = iterator.next();
					if (!car.getName().equals("s4")) {
						logger.warn("Bolt去除数据：",car);
						iterator.remove();
					}
				}
				if (listcar !=null && listcar.size()>0) {
					for (Car car : listcar) {
						carService.insertCar(car);
					}
				}
			}
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			logger.error("Bolt获取数据失败！",msg,e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO 自动生成的方法存根
		
	}

	
}
