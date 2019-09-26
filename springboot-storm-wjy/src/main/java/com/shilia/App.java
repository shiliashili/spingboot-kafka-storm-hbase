package com.shilia;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.shilia.storm.Topology;
import com.shilia.util.GetBean;

import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
public class App {
	public static void  main(String[] args) {
		//获取启动函数
		ConfigurableApplicationContext context=SpringApplication.run(App.class, args);
		
		GetBean bean1 = new GetBean();
		
		bean1.setApplicationContext(context);
		
		Topology topo = context.getBean(Topology.class);
		
		topo.run();
		
	}
}
