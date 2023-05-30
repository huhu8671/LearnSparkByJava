package learning.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingJoin {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingJoin").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(10));// Duration 表示采集周期

        // 获取端口数据
        JavaReceiverInputDStream<String> lines1 = ssc.socketTextStream("localhost", 9999);
        JavaReceiverInputDStream<String> lines2 = ssc.socketTextStream("localhost", 8888);
        JavaPairDStream<String, Integer> line1 = lines1.mapToPair(a -> new Tuple2<>(a, 1));
        JavaPairDStream<String, Integer> line2 = lines2.mapToPair(a -> new Tuple2<>(a, 1));

        line1.join(line2).print();


        ssc.start();// 启动采集器
        ssc.awaitTermination();// 等待采集器关闭
    }
}
