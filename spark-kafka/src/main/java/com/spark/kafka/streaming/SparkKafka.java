package com.spark.kafka.streaming;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class SparkKafka {
    public static void main(String[] argv) throws Exception {

        // Configure Spark to connect to Kafka running on local machine
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        //Configure Spark to listen messages in topic test
        Collection<String> topics = Arrays.asList("test");

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkKafka10WordCount");

        //Read messages in batch of 30 seconds
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(30));

        // Start reading messages from Kafka and get DStream
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));

        // Read value of each message from Kafka and return it
        JavaDStream<String> lines = stream.map(kafkaRecord -> kafkaRecord.value());

        // Break every message into words and return list of words
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // Take every word and return Tuple with (word,1)
        JavaPairDStream<String, Integer> wordMap = words.mapToPair(word -> new Tuple2<>(word, 1));


        // Count occurance of each word
        JavaPairDStream<String, Integer> wordCount = wordMap.reduceByKey((Integer first, Integer second) -> first + second);


        //Print the word count
        wordCount.print();

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}