package com.shilia.controller;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import com.alibaba.fastjson.JSONObject;
import com.shilia.entity.Car;
import com.shilia.entity.HbaseTable;
import com.shilia.kafka.KafkaProducerTest;
import com.shilia.service.CarService;
import com.shilia.util.HBaseUtil;


@Controller
public class MainController {
	
	@Autowired
	private CarService carService;
	
	@RequestMapping("/")
	public String mainmenu() {
		return "car/content";
	}
	
	@RequestMapping("kafkaproducer")
	public String test(@ModelAttribute Car msgs) {
		try {
			String topicName="KAFKA123";
			KafkaProducerTest kafkaProducerTest = new KafkaProducerTest(topicName,JSONObject.toJSONString(msgs));
			kafkaProducerTest.run();
			return "redirect:/findall";
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			return "error0";
		}
		
	}
	
	@RequestMapping("findall")
	public String test1(Model model) {
		List<Car> cars = carService.findByCar();
		model.addAttribute("cars",cars);
		return "car/list";
	}
	
	@GetMapping("kafka")
	public String kafka(Model model) {
		Car car = new Car();
		model.addAttribute("car", car);
		return "car/storm";
	}
	
	@GetMapping("hbaseform")
	public String hbaseform(Model model) {
		HbaseTable hb = new HbaseTable();
		model.addAttribute("hb", hb);
		return "car/hbaseform";
	}
	
	@GetMapping("toDelete")
	public String delete(int id) {
		carService.delete(id);
		return "redirect:/findall";
	}
	
	@RequestMapping("hbase")
	public String hbase(@ModelAttribute HbaseTable hbaseTable) {
		HBaseUtil hBaseUtil = new HBaseUtil();
		String tableName = hbaseTable.getTableName();
		String family = hbaseTable.getFamily();
		String[] columnFamily = new String[1];
		columnFamily[0]=family;
		hBaseUtil.creatTable(tableName, columnFamily);
		hBaseUtil.insert(hbaseTable.getTableName(), hbaseTable.getRowKey(), hbaseTable.getFamily(), hbaseTable.getQualifier(), hbaseTable.getValue());
		return "redirect:/findhbase";
	}
	
	@RequestMapping("findhbase")
	public String findhbase(Model model) throws IOException {
		HBaseUtil hBaseUtil = new HBaseUtil();
		HbaseTable ht = new HbaseTable();
		List<HbaseTable> list1 = new ArrayList<HbaseTable>();
		for(TableName t_name:hBaseUtil.getListTable()) {
  		  List<Map<String, Object>> select = hBaseUtil.select(t_name.getNameAsString());
  		  for(Map<String,Object> i:select) {
  			ht = JSONObject.parseObject(JSONObject.toJSONString(i), HbaseTable.class);
  			ht.setTableName(t_name.getNameAsString());
  			  list1.add(ht);
  		  }
  	  }
		model.addAttribute("list1",list1);
		return "car/hbaselist";
		
	}
	
}
