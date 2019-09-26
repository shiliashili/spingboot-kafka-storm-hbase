package com.shilia.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.shilia.storm.bolt.Bolt1;
import com.shilia.storm.spout.Spout1;

@Component
public class Topology {
	
	private final Logger logger = LoggerFactory.getLogger(Topology.class);
	
	public void run() {
		//创建一个拓扑
		try {
			TopologyBuilder builder = new TopologyBuilder();
			
			//创建一个数据源线程
			builder.setSpout("storm-spout", new Spout1(),1);
			
			//创建一个bolt线程和2个task
			builder.setBolt("storm-bolt", new Bolt1(),1).setNumTasks(1).shuffleGrouping("storm-spout");
			
			Config con = new Config();
			
			//创建一个应答
			con.setNumAckers(1);
			
			//创建一个work
			con.setNumWorkers(1);
			
			logger.info("------本地模式启动------");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Topology", con, builder.createTopology());
		} catch (IllegalArgumentException e) {
			// TODO 自动生成的 catch 块
			logger.error("------storm启动失败！------");
			System.exit(1);
		}
		
		logger.info("------storm启动成功------");
		
	}
}
