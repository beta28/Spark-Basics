package com.spark.kafka.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static java.util.Collections.singletonList;


public class SparkMongoRead {
    public static void main(String[] args) {

        //setting logger to error
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.spark")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.spark")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        /*Start Example: Read data from MongoDB************************/
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        /*End Example**************************************************/


        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { spark : { $gt : 5 } } }")));
        /*End Example**************************************************/

        // Analyze data from MongoDB
        System.out.println(aggregatedRdd.count());
        System.out.println(aggregatedRdd.first().toJson());

        jsc.close();

    }
}