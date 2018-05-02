package com.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);


        new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"))
                .textFile("src/resource/shakespeare.txt")
                .flatMap(sentences -> Arrays.asList(sentences.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(Tuple2::swap)
                .sortByKey(true)
                .mapToPair(Tuple2::swap)
                .collect()
                .forEach(System.out::println);
    }
}