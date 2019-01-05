package br.com.test.bigdata.spark.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;

public class SparkStreamingReader {

    private static final Pattern SPACE = Pattern.compile(" ");

    private static final ObjectMapper objectMappter = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String brokers = "localhost:9092";
        String topics = "K-TOPIC-NETFLIX";

        Logger.getRootLogger().setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

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
        JavaDStream<String> words = lines.map(x -> (String) objectMappter.readValue(x, Map.class).get("text"))
                .flatMap(x -> Arrays.asList(SPACE.split(x)).iterator())
                .filter(s -> {
                    String trimmed = s.trim();
                    return !"".equals(trimmed) && trimmed.length() > 1;
                }).map(String::toLowerCase);
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2).mapToPair(x -> new Tuple2<>(x._2, x._1))
                .transformToPair(x -> x.sortByKey(false)).mapToPair(x -> new Tuple2<>(x._2, x._1));
        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }

}