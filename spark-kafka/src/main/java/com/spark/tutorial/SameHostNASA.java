package com.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class SameHostNASA implements Serializable {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.
           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....
           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes
           Make sure the head lines are removed in the resulting RDD.
      */
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        JavaSparkContext javaSparkContext = new JavaSparkContext(new SparkConf().setAppName("sameHostName").setMaster("local"));
        /*javaSparkContext
                .textFile("src/resource/nasa_19950701.tsv")
                .map(lines -> lines.split("\t")[0])
                .intersection(javaSparkContext
                        .textFile("src/resource/nasa_19950801.tsv")
                        .map(lines -> lines.split("\t")[0]))
                .filter(lines -> !lines.contains("host"))
                .collect()
                .forEach(System.out::println);*/


         /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.
           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes
           Make sure the head lines are removed in the resulting RDD.
         */
        javaSparkContext
                .textFile("src/resource/nasa_19950701.tsv")
                .union(javaSparkContext
                        .textFile("src/resource/nasa_19950801.tsv"))
                .filter(lines -> !lines.startsWith("host"))
                .sample(true, 0.1)
                .collect()
                .forEach(System.out::println);
    }
}
