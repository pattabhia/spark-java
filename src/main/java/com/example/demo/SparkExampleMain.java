package com.example.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkExampleMain {

    private static final Logger log = LoggerFactory.getLogger(SparkExampleMain.class);

    public static void main(String[] args) throws InterruptedException {

        List<Integer> integerList = new ArrayList<>();

        integerList.add(30);
        integerList.add(40);
        integerList.add(50);
        integerList.add(60);
        integerList.add(70);
        integerList.add(80);
        integerList.add(80);

        SparkConf sparkConf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> originalIntegerRDD = sc.parallelize(integerList);
        Integer output = originalIntegerRDD.reduce((val1,val2) ->val1+val2);

        //log.error("SUM OF INTEGERS={}", output);
        JavaRDD<Double> sqrtRDD = originalIntegerRDD.map(value ->Math.sqrt(value));
       // sqrtRDD.collect().forEach(System.out::println);
        //sqrtRDD.foreach(value->log.error("SQRT OF NUMBERS={}",value));
        //log.info("COUNT OF ELEMENTS IN   RDD={}",sqrtRDD.count());

        JavaRDD<Integer> simpleinteger = sqrtRDD.map(value ->1);
        Integer count = simpleinteger.reduce((v1,v2)->v1+v2);
        //System.out.println("Simple integer count = "+count);

        JavaRDD<Tuple2<Integer,Double>> tuple2JavaRDD = originalIntegerRDD.map(value ->new Tuple2<>(value,Math.sqrt(value)));
        tuple2JavaRDD.foreach(v-> System.out.println(v));

    }
}
