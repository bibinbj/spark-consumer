package com.bib.sparkconsumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkProcessor {
	
	@Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;	
	
	private static final String GROUP = "testgp";
	
	public void process() throws InterruptedException {
		SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        // TODO: processing pipeline
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("test");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);


        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> System.out.println(record._2));
        });
        ssc.start();
        ssc.awaitTermination();
	}
	
//	public void processor() throws InterruptedException {
//		SparkConf sparkConf = new SparkConf();
//		sparkConf.setAppName("WordCountingApp");
//		sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
//		 
//		JavaStreamingContext streamingContext = new JavaStreamingContext(
//		  sparkConf, Durations.seconds(1));
//
//		Map<String, Object> kafkaParams = new HashMap<>();
//		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
//		kafkaParams.put("auto.offset.reset", "latest");
//		kafkaParams.put("enable.auto.commit", false);
//		Collection<String> topics = Arrays.asList("messages");
//		 
//		JavaInputDStream<ConsumerRecord<String, String>> messages = 
//		  KafkaUtils.createDirectStream(
//		    streamingContext, 
//		    LocationStrategies.PreferConsistent(), 
//		    ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
//		JavaPairDStream<String, String> results = messages
//				  .mapToPair( 
//				      record -> new Tuple2<>(record.key(), record.value())
//				  );
//				JavaDStream<String> lines = results
//				  .map(
//				      tuple2 -> tuple2._2()
//				  );
//				JavaDStream<String> words = lines
//				  .flatMap(
//				      x -> Arrays.asList(x.split("\\s+")).iterator()
//				  );
//				JavaPairDStream<String, Integer> wordCounts = words
//				  .mapToPair(
//				      s -> new Tuple2<>(s, 1)
//				  ).reduceByKey(
//				      (i1, i2) -> i1 + i2
//				    );
//
//	
//				ssc.start();
//				ssc.awaitTermination();		
//	}

}
