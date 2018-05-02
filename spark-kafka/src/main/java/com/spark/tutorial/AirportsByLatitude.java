package com.spark.tutorial;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsByLatitude {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

         /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.
           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */

        new JavaSparkContext(new SparkConf().setAppName("airportByLatitude").setMaster("local"))
                .textFile("src/resource/airports.txt")
                .filter(lines -> lines.split(",").length == 12)
                .filter(lines -> Float.parseFloat(lines.split(",")[6]) > 40)
                .mapToPair(lines -> new Tuple2<>(lines.split(",")[0], lines.split(",")[1]))
                .collect()
                .forEach(System.out::println);

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.
           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */

        new JavaSparkContext(new SparkConf().setAppName("airportByCountryName").setMaster("local"))
                .textFile("src/resource/airports.txt")
                .filter(lines -> lines.split(",").length == 12)
                .filter(lines -> lines.split(",")[3].equalsIgnoreCase("\"United States\""))
                .mapToPair(lines -> new Tuple2<>(lines.split(",")[1], lines.split(",")[2]))
                .collect()
                .forEach(System.out::println);
    }
}
