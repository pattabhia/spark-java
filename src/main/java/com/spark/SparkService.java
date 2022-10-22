package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import scala.Tuple2;

@Component
public class SparkService {

    @Value("${spark.input.file.path}")
    private String path;

    public void process() {

        SparkConf sparkConf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> initialRdd = sc.textFile(new ClassPathResource("src/main/resources/"+ path).getPath());

        System.out.println("-------------------Number of user occurences-----------------------");
        long count = initialRdd.mapToPair(x -> new Tuple2<>(x.split(",")[0], 1L)).count();
        System.out.println(count);

        sc.close();
    }
}
