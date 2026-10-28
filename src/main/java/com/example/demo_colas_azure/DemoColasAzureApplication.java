package com.example.demo_colas_azure;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DemoColasAzureApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoColasAzureApplication.class, args);
	}

}
