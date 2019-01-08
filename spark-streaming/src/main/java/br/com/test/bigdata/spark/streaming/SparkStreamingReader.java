package br.com.test.bigdata.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.text.SimpleDateFormat;
import java.util.*;

public class SparkStreamingReader {

    public static void main(String[] args) throws Exception {
        String brokers = "localhost:9092";
        String topics = "K-TOPIC-NETFLIX";

        Logger.getRootLogger().setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("JavaBigDataSparkStreamingConsumer");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.minutes(1));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "twitter-stream-reader");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        lines.foreachRDD(rdd -> {
            String format = new SimpleDateFormat("yyyy-MM-dd_HH-mm").format(new Date());
            rdd.saveAsTextFile("hdfs://localhost:9000/user/spark/streaming/twitter/" + format);
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}
