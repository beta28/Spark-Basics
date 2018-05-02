package com.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class UpperCase {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        // code
        new JavaSparkContext(new SparkConf().setAppName("toUpperCase").setMaster("local"))
                .textFile("src/resource/sentences.txt")
                .map(String::toUpperCase)
                .collect()
                .forEach(System.out::println);
    }
}
