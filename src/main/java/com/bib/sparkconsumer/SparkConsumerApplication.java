package com.bib.sparkconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
public class SparkConsumerApplication {

	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext context = SpringApplication.run(SparkConsumerApplication.class, args);
		SparkProcessor p = context.getBean(SparkProcessor.class);
		p.process();
	}

}
