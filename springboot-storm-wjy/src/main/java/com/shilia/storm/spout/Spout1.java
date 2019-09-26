package com.shilia.storm.spout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.alibaba.fastjson.JSON;
import com.shilia.entity.Car;

public class Spout1 extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4433178424656347085L;
	
	private SpoutOutputCollector collector;
	
	private static final Logger logger = LoggerFactory.getLogger(Spout1.class);
	
	private KafkaConsumer<String,String> consumer;
	
	private ConsumerRecords<String,String> msgList;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO 自动生成的方法存根
		Init();
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		// TODO 自动生成的方法存根
		for (;;) {
			try {
				msgList = consumer.poll(100);
				if (null != msgList && !msgList.isEmpty()) {
					String msg = "";
					List<Car> list=new ArrayList<Car>();
					for (ConsumerRecord<String, String> record : msgList) {
						// 原始数据
						msg = record.value();
						
						if (null == msg || "".equals(msg.trim())) {
							continue;
						}
						try{
							list.add(JSON.parseObject(msg, Car.class));
						}catch(Exception e){
							logger.error("数据格式不符!数据:{}",msg);
							continue;
						}
				     } 
					logger.info("Spout发射的数据:"+list);
					//发送到bolt中
					this.collector.emit(new Values(JSON.toJSONString(list)));
					//手动消费
					 consumer.commitAsync();
				}else{
					TimeUnit.SECONDS.sleep(3);
//					logger.info("未拉取到数据...");
				}
			} catch (Exception e) {
				logger.error("消息队列处理异常!", e);
				try {
					TimeUnit.SECONDS.sleep(10);
				} catch (InterruptedException e1) {
					logger.error("暂停失败!",e1);
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO 自动生成的方法存根
		declarer.declare(new Fields("bolt1"));
		
	}
	
	private void Init() {
		Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.19.161:6667");  
        props.put("max.poll.records", "10");
        props.put("enable.auto.commit", "false");
        props.put("group.id", "group-wjy");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        String topic="KAFKA123";
    	this.consumer.subscribe(Arrays.asList(topic));
    	logger.info("消息队列[" + topic + "] 开始初始化...");
	}

}
