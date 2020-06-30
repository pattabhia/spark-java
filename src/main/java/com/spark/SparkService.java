package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

@Component
public class SparkService {

    @Value("${spark.input.file.path}")
    private String extractionFilePath;

    public void process() {

        SparkConf sparkConf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> initialRdd = sc.textFile(extractionFilePath);

        System.out.println("-------------------Number of pincode occurences-----------------------");
        initialRdd.mapToPair(rawvalue -> new Tuple2<>(rawvalue.split(",")[4], 1L))
                .reduceByKey((v1, v2) -> v1 + v2)
                .foreach(tuple -> System.out.println(tuple._1 + " has occured " + tuple._2 + " instances"));

        System.out.println("-------------------Number of user occurences-----------------------");
        initialRdd.mapToPair(rawvalue -> new Tuple2<>(rawvalue.split(",")[0], 1L))
                .reduceByKey((v1, v2) -> v1 + v2)
                .foreach(tuple -> System.out.println(tuple._1 + "  has occured " + tuple._2 + " instances"));

        sc.close();
    }
}
