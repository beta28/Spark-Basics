package com.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class ActionTransformationSpark {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        JavaSparkContext javaSparkContext = new JavaSparkContext(new SparkConf().setAppName("action-transformation").setMaster("local"));


        /**
         * Create a Spark program to read words from general.txt, Apply take to first 3 results
         */
        JavaRDD<String> readFile = javaSparkContext.textFile("src/resource/general.txt");
        JavaRDD<String> splitLines = readFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        List<String> take = splitLines.take(3);
        System.out.println("Take");
        take.forEach(System.out::println);

        /**
         * Create a Spark program to read the first 100 prime numbers from prime_number.txt,
         * print the sum of those numbers to console.
         * Each row of the input file contains 10 prime numbers separated by spaces.
         */
        System.out.println("Sum Prime : " + javaSparkContext.textFile("src/resource/prime_number.txt")
                .flatMap(line -> Arrays.asList(line.split(",")).iterator())
                .map(Integer::parseInt)
                .reduce((x, y) -> x + y));

        /**
         * Reduce
         */
        System.out.print("Reduce : ");
        System.out.println(javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5)).reduce((x, y) -> x * y));

    }
}
