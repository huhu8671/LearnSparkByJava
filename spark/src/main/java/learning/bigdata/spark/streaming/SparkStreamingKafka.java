package learning.bigdata.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class SparkStreamingKafka {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingWC").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(3));// Duration 表示采集周期

        HashMap<String, Object> kafkaParas = new HashMap<>();
        kafkaParas.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        kafkaParas.put(ConsumerConfig.GROUP_ID_CONFIG,"topic0");
        kafkaParas.put("key.deserializer","");
        kafkaParas.put("value.deserializer","");
        KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singletonList("sparkstreaming"),kafkaParas))
                .map(a->a).print();

        ssc.start();// 启动采集器
        ssc.awaitTermination();// 等待采集器关闭
    }
}
