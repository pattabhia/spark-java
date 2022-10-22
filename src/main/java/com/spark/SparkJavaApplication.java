package com.spark;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkJavaApplication implements CommandLineRunner {

    private final SparkService sparkService;

    public SparkJavaApplication(SparkService sparkService) {
        this.sparkService = sparkService;
    }

    public static void main(String[] args) {
        SpringApplication.run(SparkJavaApplication.class, args);
    }

    @Override
    public void run(String... args) {
        sparkService.process();
    }
}
