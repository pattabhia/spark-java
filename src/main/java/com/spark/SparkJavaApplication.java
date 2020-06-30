package com.spark;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkJavaApplication implements CommandLineRunner {

    @Autowired private SparkService sparkService;

    public static void main(String[] args) {
        SpringApplication.run(SparkJavaApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        sparkService.process();
    }
}
